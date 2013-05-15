{-# LANGUAGE FlexibleInstances, FlexibleContexts, DeriveFunctor, MultiParamTypeClasses, TypeFamilies, UndecidableInstances #-}

module Test.HyperDex.Util where

import Test.QuickCheck hiding (NonEmpty, getNonEmpty)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import Data.Char (isAsciiLower)

import Control.Applicative

import Data.Set (Set)
import qualified Data.Set as Set

-- | An arbitrary bytestring

instance Arbitrary ByteString where
  arbitrary = fmap BS.pack $ arbitrary
  shrink = map BS.pack . shrink . BS.unpack 

-- | Definitions for bytestrings not containing NUL characters

notNul :: Char -> Bool
notNul '\0' = False
notNul _    = True

newtype NonNul a = NonNul { getNonNul :: a }
  deriving (Show, Eq, Functor)

instance Arbitrary (NonNul Char) where
  arbitrary = fmap NonNul $ choose ('\1', '\255')

instance Arbitrary (NonNul ByteString) where
  arbitrary = fmap (NonNul . BS.pack . fmap getNonNul) arbitrary  
  shrink (NonNul xs) = map NonNul $ filter (BS.all notNul) $ shrink xs

-- | Definitions for lower ASCII bytestrings

newtype LowerAscii a = LowerAscii { getLowerAscii :: a }
  deriving (Show, Eq, Functor)

instance Arbitrary (LowerAscii Char) where
  arbitrary = fmap LowerAscii $ choose ('a', 'z')

instance Arbitrary (LowerAscii ByteString) where
  arbitrary = fmap (LowerAscii . BS.pack . fmap getLowerAscii) arbitrary  
  shrink (LowerAscii xs) = map LowerAscii $ filter (BS.all isAsciiLower) $ shrink xs

-- | Definitions for non NonEmptyable bytestrings

newtype NonEmpty a = NonEmpty { getNonEmpty :: a }
  deriving (Show, Eq, Functor)

instance Arbitrary (NonEmpty ByteString) where
  arbitrary = fmap NonEmpty $ arbitrary `suchThat` (not . BS.null)
  shrink (NonEmpty xs) = map NonEmpty $ filter (not . BS.null) $ shrink xs

newtype Identifier a = Identifier { getIdentifier :: a }
  deriving (Show, Eq, Functor)

instance Arbitrary (Identifier ByteString) where
  arbitrary = fmap (Identifier . BS.pack . fmap getLowerAscii) $ listOf1 arbitrary
  shrink (Identifier xs) = map Identifier $ filter (\x -> BS.all isAsciiLower x && not (BS.null x)) $ shrink xs

-- | Definitions for arbitrary sets

instance (Ord a, Arbitrary [a]) => Arbitrary (Set a) where
  arbitrary = fmap Set.fromList arbitrary
  shrink = map Set.fromList . shrink . Set.toList 
