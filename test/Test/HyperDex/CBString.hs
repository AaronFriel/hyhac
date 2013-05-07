{-# LANGUAGE FlexibleInstances #-}

module Test.HyperDex.CBString (cBStringTests)
  where

import Test.Framework (testGroup, Test)
import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck
import Test.QuickCheck.Monadic

import Database.HyperDex.Internal.Util
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (pack, unpack)

import Data.Int
import Foreign.C
import Foreign.Ptr

instance Arbitrary ByteString where
  arbitrary = fmap pack (vector 64)

newtype NulTerminated a = NulTerminated { unwrapNulTerminated :: a }
  deriving (Show, Eq, Ord)

-- | Arbitrary instance for a ByteString containing no NUL characters
-- /note/: The ByteString instance is not itself NUL-terminated, rather
-- the string must not contain any NUL characters and the API will append
-- the appropriate NUL when it is converted to a CString.
instance Arbitrary (NulTerminated ByteString) where
  arbitrary = fmap (NulTerminated . pack) . vectorOf 64 $ choose ('\1', '\255')

canRoundTripArbitraryString :: Test
canRoundTripArbitraryString =
  testProperty "Can round trip an arbitrary string through the CBString API" $
    verbose $ monadicIO $ do
      input <- pick arbitrary :: PropertyM IO ByteString 
      cStringLen <- run $ newCBStringLen input
      bString <- run $ peekCBStringLen cStringLen
      assert $ input == bString

canRoundTripNulTerminatedString :: Test
canRoundTripNulTerminatedString =
  testProperty "Can round trip an arbitrary string through the CBString API" $
    monadicIO $ do
      arbit <- pick arbitrary :: PropertyM IO (NulTerminated ByteString)
      let input = unwrapNulTerminated arbit
      cString <- run $ newCBString input
      bString <- run $ peekCBString cString
      assert $ input == bString

cBStringTests :: Test
cBStringTests = testGroup "CBString API Tests"
                  [ canRoundTripArbitraryString
                  , canRoundTripNulTerminatedString
                  ]
