{-# LANGUAGE FlexibleInstances, FlexibleContexts, DeriveFunctor, MultiParamTypeClasses, TypeFamilies, UndecidableInstances, ExistentialQuantification #-}

module Test.HyperDex.Util where

import Database.HyperDex

import Test.QuickCheck hiding (NonEmpty, getNonEmpty)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import Data.Char (isAsciiLower)

import Data.Int

import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)
import qualified Data.Map as Map

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

instance (Ord k, Arbitrary [(k, v)]) => Arbitrary (Map k v) where
  arbitrary = fmap Map.fromList arbitrary
  shrink = map Map.fromList . shrink . Map.toList 

-- | A type for a pair where the second is greater than the first.

data NumericPrimitive = HyperdexInteger | HyperdexFloat

newtype HyperRelated = HyperRelated { getRelated :: (HyperSerializable, HyperSerializable, HyperSerializable, Hyperpredicate) }
  deriving (Show)

instance Arbitrary HyperRelated where
  arbitrary = do
    (predicate) <- elements [ HyperpredicateEquals
                            , HyperpredicateLessEqual
                            , HyperpredicateGreaterEqual ]
    primitiveType <- elements [ HyperdexInteger, HyperdexFloat ]
    (initial, failing, succeeding) <- 
      case primitiveType of
        HyperdexInteger -> do
          initial <- arbitrary :: Gen Int64
          let test = case predicate of 
                      HyperpredicateEquals -> (==)
                      HyperpredicateLessEqual -> (<=) 
                      HyperpredicateGreaterEqual -> (>=)
                      _ -> error "Invalid predicate"
          failing <- arbitrary `suchThat` (\x -> not $ initial `test` x)
          succeeding <-
            case predicate of
              HyperpredicateEquals -> return initial
              _ -> arbitrary `suchThat` (\x -> initial `test` x)
          return (pack initial, pack failing, pack succeeding)
        HyperdexFloat -> do
          initial <- arbitrary :: Gen Double
          let test = case predicate of 
                      HyperpredicateEquals -> (==)
                      HyperpredicateLessEqual -> (<=) 
                      HyperpredicateGreaterEqual -> (>=)
                      _ -> error "Invalid predicate"
          failing <- arbitrary `suchThat` (\x -> not $ initial `test` x)
          succeeding <-
            case predicate of
              HyperpredicateEquals -> return initial
              _ -> arbitrary `suchThat` (\x -> initial `test` x)
          return (pack initial, pack failing, pack succeeding)
    return $ HyperRelated (initial, failing, succeeding, predicate)

data HyperSerializable = forall a. (Show a, HyperSerialize a) => MkHyperSerializable a

pack :: (Eq a, Show a, HyperSerialize a) => a -> HyperSerializable
pack = MkHyperSerializable

instance Arbitrary HyperSerializable where
  arbitrary = oneof [ fmap pack (arbitrary :: Gen ByteString)
                    , fmap pack (arbitrary :: Gen Int64)
                    , fmap pack (arbitrary :: Gen Double)
                    , fmap pack (arbitrary :: Gen (Set ByteString))
                    , fmap pack (arbitrary :: Gen (Set Int64))
                    , fmap pack (arbitrary :: Gen (Set Double))
                    , fmap pack (arbitrary :: Gen (Map ByteString ByteString))
                    , fmap pack (arbitrary :: Gen (Map ByteString Int64))
                    , fmap pack (arbitrary :: Gen (Map ByteString Double))
                    , fmap pack (arbitrary :: Gen (Map Int64 ByteString))
                    , fmap pack (arbitrary :: Gen (Map Int64 Int64))
                    , fmap pack (arbitrary :: Gen (Map Int64 Double))
                    , fmap pack (arbitrary :: Gen (Map Double ByteString))
                    , fmap pack (arbitrary :: Gen (Map Double Int64))
                    , fmap pack (arbitrary :: Gen (Map Double Double)) ]

instance Show HyperSerializable where
  show (MkHyperSerializable a) = show a

data NumericAtomicOp = AtomicAdd
                     | AtomicSub
                     | AtomicMul
                     | AtomicDiv
                     | AtomicMod
                     | AtomicAnd
                     | AtomicOr
                     | AtomicXor
  deriving (Show, Eq)

instance Arbitrary NumericAtomicOp where
  arbitrary = elements [ AtomicAdd, AtomicSub
                       , AtomicMul, AtomicDiv
                       , AtomicMod
                       , AtomicAnd, AtomicOr
                       , AtomicXor
                       ]
