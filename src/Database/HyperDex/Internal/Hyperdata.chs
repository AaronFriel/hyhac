{-# LANGUAGE FlexibleInstances, FlexibleContexts, UndecidableInstances, DataKinds #-}

module Database.HyperDex.Internal.Hyperdata
  ( Hyper (..)
  , HyperSerialize (..) )
  where

{# import Database.HyperDex.Internal.Hyperdex #}

import Data.Int
import Data.Word
import Control.Monad

import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)
import qualified Data.Map as Map

import Data.Serialize

import Debug.Trace

newtype Hyper a = Hyper { unHyper :: a }

instance HyperSerialize a => Serialize (Hyper a) where
  get = fmap Hyper $ remaining >>= getH
  put = putH . unHyper

class HyperSerialize a where
  getH :: Int -> Get a
  putH :: a -> Put
  datatype :: a -> Hyperdatatype

instance HyperSerialize Int64 where
  getH = const $ liftM fromIntegral getWord64le
  putH = putWord64le . fromIntegral
  datatype = const HyperdatatypeInt64

instance HyperSerialize Word32 where
  getH = const $ liftM fromIntegral getWord32le
  putH = putWord32le . fromIntegral
  datatype = const HyperdatatypeInt64

instance HyperSerialize Double where
  getH = const getFloat64le
  putH = putFloat64le
  datatype = const HyperdatatypeFloat

instance HyperSerialize ByteString where
  getH = getByteString
  putH = putByteString
  datatype = const HyperdatatypeString

instance HyperSerialize [Int64] where
  getH 0 = return []
  getH i = do
    first <- getH i :: Get Int64
    rest <- getH (i-8) :: Get [Int64]
    return $ first : rest
  putH []     = return ()
  putH (x:xs) = putH x >> putH xs 
  datatype = const HyperdatatypeListInt64

instance HyperSerialize [Double] where
  getH i | i <= 0    = return []
         | otherwise = do
    first <- getH i :: Get Double
    rest <- getH (i-8) :: Get [Double]
    return $ first : rest
  putH []     = return ()
  putH (x:xs) = putH x >> putH xs
  datatype = const HyperdatatypeListFloat

instance HyperSerialize [ByteString] where
  getH 0 = return []
  getH i = do
    len <- getH i :: Get Word32
    first <- getByteString (fromIntegral len)
    rest <- getH (i - (fromIntegral len) - 4) :: Get [ByteString]
    return $ first : rest
  putH []     = return ()
  putH (x:xs) = do
    (putH :: Word32 -> Put) . fromIntegral . ByteString.length $ x
    putByteString x
    putH xs
  datatype = const HyperdatatypeListString

setGet :: (Ord a, HyperSerialize [a]) => Int -> Get (Set a)
setGet = fmap Set.fromList . getH

setPut :: (Ord a, HyperSerialize [a]) => Set a -> Put
setPut = mapM_ (\a -> putH [a]) . Set.toList

instance HyperSerialize (Set Int64) where
  getH = setGet
  putH = setPut 
  datatype = const HyperdatatypeSetInt64

instance HyperSerialize (Set Double) where
  getH = setGet
  putH = setPut 
  datatype = const HyperdatatypeSetFloat

instance HyperSerialize (Set ByteString) where
  getH = setGet
  putH = setPut 
  datatype = const HyperdatatypeSetString

hyperMapGet :: (HyperSerialize [a], HyperSerialize [b]) => Int -> Get [(a,b)]
hyperMapGet i | i <= 0    = return []
              | otherwise = do
                  [key] <- getH i
                  [value] <- getH (i-8)
                  rest <- hyperMapGet (i-16)
                  return $ (key,value) : rest

mapGet :: (Ord k, HyperSerialize [k], HyperSerialize [v]) => Int -> Get (Map k v)
mapGet = fmap Map.fromList . hyperMapGet

mapPut :: (Ord k, HyperSerialize [k], HyperSerialize [v]) => Map k v -> Put
mapPut = mapM_ (\(k,v) -> putH [k] >> putH [v]) . Map.toList

instance HyperSerialize (Map Int64 Int64) where
  getH = mapGet
  putH = mapPut
  datatype = const HyperdatatypeMapInt64Int64

instance HyperSerialize (Map Int64 Double) where
  getH = mapGet
  putH = mapPut
  datatype = const HyperdatatypeMapInt64Int64

instance HyperSerialize (Map Int64 ByteString) where
  getH = mapGet
  putH = mapPut
  datatype = const HyperdatatypeMapInt64String

instance HyperSerialize (Map Double Int64) where
  getH = mapGet
  putH = mapPut
  datatype = const HyperdatatypeMapFloatInt64

instance HyperSerialize (Map Double Double) where
  getH = mapGet
  putH = mapPut
  datatype = const HyperdatatypeMapFloatFloat

instance HyperSerialize (Map Double ByteString) where
  getH = mapGet
  putH = mapPut
  datatype = const HyperdatatypeMapFloatString

instance HyperSerialize (Map ByteString Int64) where
  getH = mapGet
  putH = mapPut
  datatype = const HyperdatatypeMapStringInt64

instance HyperSerialize (Map ByteString Double) where
  getH = mapGet
  putH = mapPut
  datatype = const HyperdatatypeMapStringFloat

instance HyperSerialize (Map ByteString ByteString) where
  getH = mapGet
  putH = mapPut
  datatype = const HyperdatatypeMapStringString

{-
toList :: a -> [a]
toList a = [a]


stringListRep :: String -> [Builder]
stringListRep str =
  let bytes = toByteString str in 
    binaryRep (fromIntegral . ByteString.length $ bytes :: Int64)
    ++ [byteString bytes]

instance Hyperdata (Set String) where
  datatype  = const HyperdatatypeListInt64
  binaryRep = concat . map (stringListRep) . Set.toList

mapListRep :: (Hyperdata k, Hyperdata v) => (k, v) -> [Builder]
mapListRep (key, value) = binaryRep key ++ binaryRep value

instance Hyperdata (Map Int64 Int64) where
  datatype  = const HyperdatatypeMapInt64Int64
  binaryRep = concat . map mapListRep . Map.toList
-}