{-# LANGUAGE FlexibleInstances, FlexibleContexts, UndecidableInstances, DataKinds #-}

module Database.HyperDex.Internal.Hyperdata
  ( Hyper (..) )
  where

import Data.Int
import Control.Monad

import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)
import qualified Data.Map as Map

import Data.Serialize

newtype Hyper a = Hyper { unHyper :: a }

instance HyperSerialize a => Serialize (Hyper a) where
  get = fmap Hyper $ remaining >>= getH
  put = putH . unHyper

class HyperSerialize a where
  getH :: Int -> Get a
  putH :: a -> Put

instance HyperSerialize Int64 where
  getH = const $ liftM fromIntegral getWord64le
  putH = putWord64le . fromIntegral

instance HyperSerialize Double where
  getH = const $ getFloat64le
  putH = putFloat64le

instance HyperSerialize ByteString where
  getH = getByteString
  putH = putByteString

instance HyperSerialize [Int64] where
  getH 0 = return []
  getH i = do
    first <- getH i :: Get Int64
    rest <- getH (i-8) :: Get [Int64]
    return $ first : rest
  putH []     = return ()
  putH (x:xs) = putH x >> putH xs 

instance HyperSerialize [Double] where
  getH i | i <= 0    = return []
         | otherwise = do
    first <- getH i :: Get Double
    rest <- getH (i-8) :: Get [Double]
    return $ first : rest
  putH []     = return ()
  putH (x:xs) = putH x >> putH xs

instance HyperSerialize [ByteString] where
  getH 0 = return []
  getH i = do
    len <- getH i :: Get Int64
    first <- getByteString (fromIntegral len)
    rest <- getH i :: Get [ByteString]
    return $ first : rest
  putH []     = return ()
  putH (x:xs) = do
    (putH :: Int64 -> Put) . fromIntegral . ByteString.length $ x
    putByteString x
    putH xs

instance (Ord a, HyperSerialize [a]) => HyperSerialize (Set a) where
  getH = fmap Set.fromList . getH
  putH = mapM_ (\a -> putH [a]) . Set.toList

hyperMapGet :: (HyperSerialize [a], HyperSerialize [b]) => Int -> Get [(a,b)]
hyperMapGet i | i <= 0    = return []
              | otherwise = do
                  [key] <- getH i
                  [value] <- getH (i-8)
                  rest <- hyperMapGet (i-16)
                  return $ (key,value) : rest

instance (Ord k, HyperSerialize [k], HyperSerialize [v]) => HyperSerialize (Map k v) where
  getH = fmap Map.fromList . hyperMapGet
  putH = mapM_ (\(k,v) -> putH [k] >> putH [v]) . Map.toList

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