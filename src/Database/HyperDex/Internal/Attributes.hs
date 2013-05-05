{-# LANGUAGE FlexibleInstances #-}

module Database.HyperDex.Internal.Attributes
  where

import Data.Int
import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Lazy.Builder
import Database.HyperDex.Internal.Hyperdex
import Data.Monoid

import Data.Set (Set)
import qualified Data.Set as Set

import Data.Map (Map)
import qualified Data.Map as Map

class Hyperdata a where
  datatype  :: a -> Hyperdatatype
  binaryRep :: a -> [Builder]
  toByteString :: a -> ByteString
  toByteString = toStrict . toLazyByteString . mconcat . binaryRep

toList :: a -> [a]
toList a = [a]

instance Hyperdata Int64 where
  datatype  = const HyperdatatypeInt64
  binaryRep = toList . int64LE

instance Hyperdata Double where
  datatype  = const HyperdatatypeFloat
  binaryRep = toList . doubleLE

instance Hyperdata String where
  datatype  = const HyperdatatypeString
  binaryRep = toList . stringUtf8

instance Hyperdata [Int64] where
  datatype  = const HyperdatatypeListInt64
  binaryRep = concat . map binaryRep

instance Hyperdata [Double] where
  datatype  = const HyperdatatypeListInt64
  binaryRep = concat . map binaryRep

stringListRep :: String -> [Builder]
stringListRep str =
  let bytes = toByteString str in 
    binaryRep (fromIntegral . ByteString.length $ bytes :: Int64)
    ++ [byteString bytes]

instance Hyperdata [String] where
  datatype  = const HyperdatatypeListInt64
  binaryRep = concat . map stringListRep

instance Hyperdata (Set Int64) where
  datatype  = const HyperdatatypeSetInt64
  binaryRep = concat . map binaryRep . Set.toList

instance Hyperdata (Set Double) where
  datatype  = const HyperdatatypeSetInt64
  binaryRep = concat . map binaryRep . Set.toList

instance Hyperdata (Set String) where
  datatype  = const HyperdatatypeListInt64
  binaryRep = concat . map (stringListRep) . Set.toList

mapListRep :: (Hyperdata k, Hyperdata v) => (k, v) -> [Builder]
mapListRep (key, value) = binaryRep key ++ binaryRep value

instance Hyperdata (Map Int64 Int64) where
  datatype  = const HyperdatatypeMapInt64Int64
  binaryRep = concat . map mapListRep . Map.toList

instance Hyperdata (Map Int64 Double) where
  datatype  = const HyperdatatypeMapInt64Float
  binaryRep = concat . map mapListRep . Map.toList

instance Hyperdata (Map Int64 String) where
  datatype  = const HyperdatatypeMapInt64String
  binaryRep = concat . map mapListRep . Map.toList

instance Hyperdata (Map Double Int64) where
  datatype  = const HyperdatatypeMapFloatInt64
  binaryRep = concat . map mapListRep . Map.toList

instance Hyperdata (Map Double Double) where
  datatype  = const HyperdatatypeMapFloatFloat
  binaryRep = concat . map mapListRep . Map.toList

instance Hyperdata (Map Double String) where
  datatype  = const HyperdatatypeMapFloatString
  binaryRep = concat . map mapListRep . Map.toList

instance Hyperdata (Map String Int64) where
  datatype  = const HyperdatatypeMapStringInt64
  binaryRep = concat . map mapListRep . Map.toList

instance Hyperdata (Map String Double) where
  datatype  = const HyperdatatypeMapStringFloat
  binaryRep = concat . map mapListRep . Map.toList

instance Hyperdata (Map String String) where
  datatype  = const HyperdatatypeMapStringString
  binaryRep = concat . map mapListRep . Map.toList
