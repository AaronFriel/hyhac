{-# LANGUAGE FlexibleInstances, FlexibleContexts, UndecidableInstances, DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
-- |
-- Module     	: Database.HyperDex.Internal.Hyperdata
-- Copyright  	: (c) Aaron Friel 2013-2014
-- License    	: BSD-style
-- Maintainer 	: mayreply@aaronfriel.com
-- Stability  	: unstable
-- Portability	: portable
--
module Database.HyperDex.Internal.Hyperdata
  ( Hyper (..)
  , HyperSerialize (..)
  , serialize
  , deserialize
  )
  where

{# import Database.HyperDex.Internal.Hyperdex #}

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

serialize :: HyperSerialize a => a -> ByteString
serialize = runPut . put . Hyper

deserialize :: HyperSerialize a => ByteString -> Either String a
deserialize = fmap unHyper . runGet get

instance HyperSerialize a => Serialize (Hyper a) where
  get = fmap Hyper getH
  put = putH . unHyper

class HyperSerialize a where
  getH :: Get a
  putH :: a -> Put
  datatype :: a -> Hyperdatatype

instance HyperSerialize Int64 where
  getH = liftM fromIntegral getWord64le
  putH = putWord64le . fromIntegral
  datatype = const HyperdatatypeInt64

instance HyperSerialize Double where
  getH = getFloat64le
  putH = putFloat64le
  datatype = const HyperdatatypeFloat

instance HyperSerialize ByteString where
  getH = remaining >>= getByteString
  putH = putByteString
  datatype = const HyperdatatypeString

newtype ListElem a = ListElem { unListElem :: a }

instance Serialize (ListElem Int64) where
  get = fmap (ListElem . fromIntegral) getWord64le
  put = putWord64le . fromIntegral . unListElem

instance Serialize (ListElem Double) where
  get = fmap ListElem getFloat64le
  put = putFloat64le . unListElem

instance Serialize (ListElem ByteString) where
  get = do
   len <- getWord32le
   fmap ListElem $ getByteString (fromIntegral len)
  put (ListElem x) = do
   putWord32le $ fromIntegral $ ByteString.length x
   putByteString x

listGet :: (Serialize (ListElem a)) => Get [a]
listGet = do
  i <- remaining
  case i <= 0 of
    True  -> return []
    False -> do
      first <- fmap unListElem get
      rest <- listGet
      return $ first : rest

listPut :: (Serialize (ListElem a)) => [a] -> Put 
listPut []     = return ()
listPut (x:xs) = (put $ ListElem x) >> listPut xs

instance HyperSerialize [Int64] where
  getH = listGet
  putH = listPut
  datatype = const HyperdatatypeListInt64

instance HyperSerialize [Double] where
  getH = listGet
  putH = listPut
  datatype = const HyperdatatypeListFloat

instance HyperSerialize [ByteString] where
  getH = listGet
  putH = listPut
  datatype = const HyperdatatypeListString

setGet :: (Ord a, HyperSerialize [a]) => Get (Set a)
setGet = fmap Set.fromList getH

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

mapListGet :: (Show k, Show v, Ord k, Serialize (ListElem k), Serialize (ListElem v)) => Get [(k, v)]
mapListGet = do
  i <- remaining
  case i <= 0 of
    True  -> return [] 
    False -> do
      key <- fmap unListElem get
      value <- fmap unListElem get
      rest <- mapListGet
      return $ (key, value) : rest

mapListPut :: (Show k, Show v, Ord k, Serialize (ListElem k), Serialize (ListElem v)) => [(k,v)]-> Put
mapListPut ((k,v):xs) = do
  put $ ListElem k
  put $ ListElem v
  mapListPut xs
mapListPut []         = return ()

mapGet :: (Show k, Show v, Ord k, Serialize (ListElem k), Serialize (ListElem v)) => Get (Map k v)
mapGet = fmap Map.fromList mapListGet

mapPut :: (Show k, Show v, Ord k, Serialize (ListElem k), Serialize (ListElem v)) => Map k v -> Put
mapPut = mapListPut . Map.toList

instance HyperSerialize (Map Int64 Int64) where
  getH = mapGet
  putH = mapPut
  datatype = const HyperdatatypeMapInt64Int64

instance HyperSerialize (Map Int64 Double) where
  getH = mapGet
  putH = mapPut
  datatype = const HyperdatatypeMapInt64Float

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