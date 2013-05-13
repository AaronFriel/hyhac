{-# LANGUAGE FlexibleInstances, FlexibleContexts, DeriveFunctor, MultiParamTypeClasses #-}

module Test.HyperDex.Util where

import Test.QuickCheck hiding (NonEmpty)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import Data.Char (isAsciiLower)

import Control.Applicative

notNul :: Char -> Bool
notNul '\0' = False
notNul _    = True

newtype NonNul a = NonNul { getNonNul :: a }
  deriving (Show, Eq, Functor)

lowerAsciiChar :: Gen (LowerAscii Char)
lowerAsciiChar = fmap LowerAscii $ choose ('a','z')

instance Arbitrary (NonNul Char) where
  arbitrary = fmap NonNul $ choose ('\1','\255')

instance Arbitrary (NonNul ByteString) where
  arbitrary = fmap (NonNul . BS.pack . fmap getNonNul) arbitrary
  shrink (NonNul xs) = map NonNul $ filter (BS.all notNul) $ shrink xs

newtype LowerAscii a = LowerAscii { getLowerAscii :: a }
  deriving (Show, Eq, Functor)

instance Arbitrary (LowerAscii Char) where
  arbitrary = fmap LowerAscii $ choose ('a','z')

instance Arbitrary (LowerAscii ByteString) where
  arbitrary = fmap (LowerAscii . BS.pack . fmap getLowerAscii) arbitrary
  shrink (LowerAscii xs) = map LowerAscii $ filter (BS.all isAsciiLower) $ shrink xs 

newtype NonEmpty a = NonEmpty { getNonEmpty :: a }
  deriving (Show, Eq, Functor)

class Empty a where
  nonEmpty :: a -> Bool

instance Empty ByteString where
  nonEmpty = not . BS.null

class Functor f => Wrapper f a where
  wrap :: a -> f a
  unWrap :: f a -> a

instance Wrapper NonNul a where
  wrap = NonNul
  unWrap = getNonNul

instance Wrapper LowerAscii a where
  wrap = LowerAscii
  unWrap = getLowerAscii

instance (Wrapper f a, Arbitrary (f a), Empty a) => Arbitrary (NonEmpty (f a)) where
  arbitrary = fmap NonEmpty $ arbitrary `suchThat` (\x -> nonEmpty $ unWrap x)

-- instance Arbitrary (NonEmpty ByteString) where
--   arbitrary = fmap NonEmpty $ arbitrary `suchThat` nonEmpty
--   shrink (NonEmpty xs) = map NonEmpty $ filter nonEmpty $ shrink xs
-- 
-- instance (Functor f, Arbitrary (f a), Empty a) => Arbitrary (NonEmpty (f a)) where
--   arbitrary = fmap NonEmpty $ fmap (liftA (`suchThat` nonEmpty)) $ arbitrary

arbitraryByteStringIdentifier :: Gen ByteString
arbitraryByteStringIdentifier = fmap getLowerAscii arbitrary

arbitraryNonNulByteString :: Gen ByteString
arbitraryNonNulByteString = fmap getNonNul arbitrary

arbitraryByteString :: Gen ByteString
arbitraryByteString = arbitrary

bsFilter :: (String -> Bool) -> ByteString -> Bool
bsFilter pred = pred . BS.unpack

instance Arbitrary ByteString where
  arbitrary = fmap BS.pack $ arbitrary
  -- | Remove substrings of decreasing size, a la quicksort:
  shrink = map BS.pack . shrink . BS.unpack 
