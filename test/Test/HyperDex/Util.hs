{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings #-}
module Test.HyperDex.Util where

import Database.HyperDex
import Database.HyperDex.Utf8

import Test.QuickCheck
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import Data.Char (isAsciiLower)

import Data.Int

import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)
import qualified Data.Map as Map

newtype DefaultSpaceAttributes = DefaultSpaceAttributes { unDefaultSpace :: [Attribute] }
  deriving (Show, Eq)

instance Arbitrary DefaultSpaceAttributes where
  arbitrary = do
    (first', last', score', profile_views',
      (pending_requests', rankings', todolist', hobbies',
        (imonafloat', friendids', unread_messages', upvotes',
          (friendranks', posts', friendremapping', intfloatmap',
            (still_looking', for_a_reason', for_float_keyed_map')))))
      <- arbitrary
    return $ DefaultSpaceAttributes
                [ mkAttributeUtf8 "first"               (first'               :: ByteString               )
                , mkAttributeUtf8 "last"                (last'                :: ByteString               )
                , mkAttributeUtf8 "score"               (score'               :: Double                   )
                , mkAttributeUtf8 "profile_views"       (profile_views'       :: Int64                    )
                , mkAttributeUtf8 "pending_requests"    (pending_requests'    :: [ByteString]             )
                , mkAttributeUtf8 "rankings"            (rankings'            :: [Double]                 )
                , mkAttributeUtf8 "todolist"            (todolist'            :: [Int64]                  )
                , mkAttributeUtf8 "hobbies"             (hobbies'             :: Set ByteString           )
                , mkAttributeUtf8 "imonafloat"          (imonafloat'          :: Set Double               )
                , mkAttributeUtf8 "friendids"           (friendids'           :: Set Int64                )
                , mkAttributeUtf8 "unread_messages"     (unread_messages'     :: Map ByteString ByteString)
                , mkAttributeUtf8 "upvotes"             (upvotes'             :: Map ByteString Int64     )
                , mkAttributeUtf8 "friendranks"         (friendranks'         :: Map ByteString Double    )
                , mkAttributeUtf8 "posts"               (posts'               :: Map Int64      ByteString)
                , mkAttributeUtf8 "friendremapping"     (friendremapping'     :: Map Int64      Int64     )
                , mkAttributeUtf8 "intfloatmap"         (intfloatmap'         :: Map Int64      Double    )
                , mkAttributeUtf8 "still_looking"       (still_looking'       :: Map Double     ByteString)
                , mkAttributeUtf8 "for_a_reason"        (for_a_reason'        :: Map Double     Int64     )
                , mkAttributeUtf8 "for_float_keyed_map" (for_float_keyed_map' :: Map Double     Double    )
                ]

pickAttributeName :: HyperSerialize a => a -> ByteString
pickAttributeName value =
  case datatype value of 
    HyperdatatypeString           -> "first"
    HyperdatatypeInt64            -> "profile_views"
    HyperdatatypeFloat            -> "score"
    HyperdatatypeListString       -> "pending_requests"
    HyperdatatypeListInt64        -> "todolist"
    HyperdatatypeListFloat        -> "rankings"
    HyperdatatypeSetString        -> "hobbies"
    HyperdatatypeSetInt64         -> "friendids"
    HyperdatatypeSetFloat         -> "imonafloat"
    HyperdatatypeMapStringString  -> "unread_messages"
    HyperdatatypeMapStringInt64   -> "upvotes"
    HyperdatatypeMapStringFloat   -> "friendranks"
    HyperdatatypeMapInt64String   -> "posts"
    HyperdatatypeMapInt64Int64    -> "friendremapping"
    HyperdatatypeMapInt64Float    -> "intfloatmap"
    HyperdatatypeMapFloatString   -> "still_looking"
    HyperdatatypeMapFloatInt64    -> "for_a_reason"
    HyperdatatypeMapFloatFloat    -> "for_float_keyed_map"
    _                             -> error "Invalid data type"

keyAttributeName :: ByteString
keyAttributeName = "username"

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

-- | Definitions for non NonEmptyBSable bytestrings

newtype NonEmptyBS a = NonEmptyBS { getNonEmptyBS :: a }
  deriving (Show, Eq, Functor)

instance Arbitrary (NonEmptyBS ByteString) where
  arbitrary = fmap NonEmptyBS $ arbitrary `suchThat` (not . BS.null)
  shrink (NonEmptyBS xs) = map NonEmptyBS $ filter (not . BS.null) $ shrink xs

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
  deriving (Show, Eq)

instance Arbitrary NumericAtomicOp where
  arbitrary = elements [ AtomicAdd, AtomicSub
                       , AtomicMul, AtomicDiv
                       ]

data IntegralAtomicOp = Numeric NumericAtomicOp
                      | AtomicMod
                      | AtomicAnd
                      | AtomicOr
                      | AtomicXor
  deriving (Show, Eq)

newtype FloatTest = FloatTest { unFloatTest :: (Double, Double, NumericAtomicOp) }
  deriving (Show, Eq)

instance Arbitrary FloatTest where
  arbitrary = do
    op <- arbitrary
    a <- arbitrary
    b <-
      case op of
        AtomicDiv -> arbitrary `suchThat` (/= 0)
        _         -> arbitrary
    return $ FloatTest (a, b, op)

instance Arbitrary IntegralAtomicOp where
  arbitrary = elements [ Numeric AtomicAdd, Numeric AtomicSub
                       , Numeric AtomicMul, Numeric AtomicDiv
                       , AtomicMod
                       , AtomicAnd, AtomicOr
                       , AtomicXor
                       ]

newtype IntegralTest = IntegralTest { unIntegralTest :: (Int64, Int64, IntegralAtomicOp) }
  deriving (Show, Eq)

instance Arbitrary IntegralTest where
  arbitrary = do
    op <- arbitrary
    a <- arbitrary
    b <-
      case op of
        Numeric AtomicAdd -> case compare a 0 of
                              GT -> choose (    minBound, maxBound + a)
                              LT -> choose (a - minBound, maxBound    )
                              EQ -> arbitrary
        Numeric AtomicSub -> case compare a 0 of
                              GT -> choose (minBound + a, maxBound    )
                              LT -> choose (minBound    , minBound + a)
                              EQ -> arbitrary
        Numeric AtomicMul -> case compare a 0 of
                              -- 
                              GT -> choose (-1 * ( maxBound `div` a),          maxBound `div` a )
                              -- 
                              LT -> choose (     (-maxBound) `div` a , -1 * ((-maxBound) `div` a))
                              EQ -> arbitrary
        Numeric AtomicDiv -> arbitrary `suchThat` (/= 0)
        AtomicMod         -> arbitrary `suchThat` (/= 0)
        _                 -> arbitrary
    return $ IntegralTest (a, b, op)

newtype SafeAddInt64 = SafeAddInt64 { safeAddInt64 :: (Int64, Int64) }
  deriving (Show)
  
instance Arbitrary SafeAddInt64 where
  arbitrary = do
    a <- arbitrary
    b <- case compare a 0 of
            GT -> choose (    minBound, maxBound + a)
            LT -> choose (a - minBound, maxBound    )
            EQ -> arbitrary
    return $ SafeAddInt64 (a, b)

newtype SafeSubtractInt64 = SafeSubtractInt64 { safeSubtractInt64 :: (Int64, Int64) }
  deriving (Show)
  
instance Arbitrary SafeSubtractInt64 where
  arbitrary = do
    a <- arbitrary
    b <- case compare a 0 of
            GT -> choose (minBound + a, maxBound    )
            LT -> choose (minBound    , minBound + a)
            EQ -> arbitrary
    return $ SafeSubtractInt64 (a, b)

newtype SafeMultiplyInt64 = SafeMultiplyInt64 { safeMultiplyInt64 :: (Int64, Int64) }
  deriving (Show)

instance Arbitrary SafeMultiplyInt64 where
  arbitrary = do
    a <- arbitrary
    b <- case compare a 0 of
            GT -> choose (-1 * ( maxBound `div` a),          maxBound `div` a )
            LT -> choose (     (-maxBound) `div` a , -1 * ((-maxBound) `div` a))
            EQ -> arbitrary
    return $ SafeMultiplyInt64 (a, b)

newtype SafeDivideInt64 = SafeDivideInt64 { safeDivideInt64 :: (Int64, Int64) }
  deriving (Show)

instance Arbitrary SafeDivideInt64 where
  arbitrary = do
    a <- arbitrary
    b <- arbitrary `suchThat` (/= 0)
    return $ SafeDivideInt64 (a, b)

newtype SafeDivideDouble = SafeDivideDouble { safeDivideDouble :: (Double, Double) }
  deriving (Show)

instance Arbitrary SafeDivideDouble where
  arbitrary = do
    a <- arbitrary
    b <- arbitrary `suchThat` (/= 0.0)
    return $ SafeDivideDouble (a, b)

newtype OverlappingMaps k v = OverlappingMaps { overlappingMaps :: (Map k v, Map k v) }
  deriving (Show)

combinations :: [a] -> [[a]]
combinations []     = [[]]
combinations (x:xs) = let cs = combinations xs in cs ++ (map (x:) cs)

instance (Ord k, Show k, Show v, Arbitrary k, Arbitrary v) => Arbitrary (OverlappingMaps k v) where
  arbitrary = do
    a <- arbitrary
    bkeys   <- elements . take 1000 . combinations . Map.keys $ a
    bvalues <- vectorOf (length bkeys) arbitrary
    let b = Map.fromList $ zip bkeys bvalues
    return $ OverlappingMaps (a, b)
  shrink (OverlappingMaps (a, b)) = 
    [ OverlappingMaps (a', b')
    | a' <- shrink a
    , let b' = Map.intersection b a'
    ]

newtype KeyMap k v = KeyMap { unKeyMap :: (Map k v, Map ByteString k) }
  deriving (Show)

instance (Ord k, Show k, Show v, Arbitrary k, Arbitrary v, HyperSerialize (Map k v)) => Arbitrary (KeyMap k v) where
  arbitrary = do
    a <- arbitrary
    let bkeys = repeat $ pickAttributeName a
    bvalues <- elements . take 1000 . combinations . Map.keys $ a
    let b = Map.fromList $ zip bkeys bvalues
    return $ KeyMap (a, b)
  shrink (KeyMap (a, b)) = 
    [ KeyMap (a', b')
    | a' <- shrink a
    , let b' = Map.intersection b a'
    ]

newtype NonOverlappingMaps k v = NonOverlappingMaps { nonOverlappingMaps :: (Map k v, Map k v) }
  deriving (Show)

instance (Ord k, Arbitrary k, Arbitrary v) => Arbitrary (NonOverlappingMaps k v) where
  arbitrary = do
    a <- arbitrary
    b <- arbitrary `suchThat` (all (\k -> not $ k `Map.member` a) . Map.keys)
    return $ NonOverlappingMaps (a, b)

minInt64, maxInt64 :: Integer
minInt64 = fromIntegral (minBound :: Int64)
maxInt64 = fromIntegral (maxBound :: Int64)

inBoundsInt64 :: Integer -> Bool
inBoundsInt64 x = x >= minInt64 && x <= maxInt64

checkMaps :: Ord k => (a -> a -> Bool) -> (Map k a, Map k a) -> (Map k a, Map k a)
checkMaps op (mapa, mapb) = (mapa, Map.filterWithKey go mapb)
  where go k _ = case (k `Map.lookup` mapa, k `Map.lookup` mapb) of
                  (Just a, Just b) -> op a b
                  _                -> True

checkAddition :: Ord k => (Map k Int64, Map k Int64) -> (Map k Int64, Map k Int64)
checkAddition = checkMaps $ \a b -> inBoundsInt64 (fromIntegral a + fromIntegral b)

checkSubtraction :: Ord k => (Map k Int64, Map k Int64) -> (Map k Int64, Map k Int64)
checkSubtraction = checkMaps $ \a b -> inBoundsInt64 (fromIntegral a - fromIntegral b)

checkMultiplication :: Ord k => (Map k Int64, Map k Int64) -> (Map k Int64, Map k Int64)
checkMultiplication = checkMaps $ \a b -> inBoundsInt64 (fromIntegral a * fromIntegral b)

checkIntDivision :: Ord k => (Map k Int64, Map k Int64) -> (Map k Int64, Map k Int64)
checkIntDivision = checkMaps $ \_ b -> b /= 0

checkDoubleDivision :: Ord k => (Map k Double, Map k Double) -> (Map k Double, Map k Double)
checkDoubleDivision = checkMaps $ \_ b -> b /= 0.0
