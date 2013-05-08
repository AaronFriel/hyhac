module Test.HyperDex.CBString (cBStringTests)
  where

import Test.Framework (testGroup, Test)
import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck
import Test.QuickCheck.Monadic

import Test.HyperDex.Util

import Database.HyperDex.Internal.Util
import Data.ByteString (ByteString)

import Data.Int
import Foreign.C
import Foreign.Ptr

canRoundTripArbitraryString :: Test
canRoundTripArbitraryString =
  testProperty "Can round trip an arbitrary string through the CBString API" $
    monadicIO $ do
      input <- pick . resize 1024 $ arbitraryByteString AllowEmpty
      cStringLen <- run $ newCBStringLen input
      bString <- run $ peekCBStringLen cStringLen
      assert $ input == bString

canRoundTripNulTerminatedString :: Test
canRoundTripNulTerminatedString =
  testProperty "Can round trip a non-nul containing string through the CBString API" $
    monadicIO $ do
      input <- pick . resize 1024 $ arbitraryNonNulByteString AllowEmpty
      cString <- run $ newCBString input
      bString <- run $ peekCBString cString
      assert $ input == bString

cBStringTests :: Test
cBStringTests = testGroup "CBString API Tests"
                  [ canRoundTripArbitraryString
                  , canRoundTripNulTerminatedString
                  ]
