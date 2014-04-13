{-# LANGUAGE RecordWildCards #-}
-- |
-- Module     	: Database.HyperDex.Internal.MapAttribute
-- Copyright  	: (c) Aaron Friel 2013-2014
--            	  (c) Niklas Hambüchen 2013-2014 
-- License    	: BSD-style
-- Maintainer 	: mayreply@aaronfriel.com
-- Stability  	: unstable
-- Portability	: portable
--
module Database.HyperDex.Internal.MapAttribute
  ( MapAttribute (..)
  , MapAttributePtr
  , mkMapAttribute
  , mkMapAttributesFromMap
  , rNewMapAttributeArray
  )
  where

import Foreign
import Foreign.C

import Data.ByteString (ByteString, packCString, packCStringLen)

import Database.HyperDex.Internal.Hyperdex
import Database.HyperDex.Internal.Hyperdata
import Database.HyperDex.Internal.Util

import Data.Map (Map)
import qualified Data.Map as Map

import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import Control.Applicative ((<$>), (<*>))

#include "hyperdex/client.h"

#c
typedef struct hyperdex_client_map_attribute hyperdex_client_map_attribute_struct;
#endc

{# pointer *hyperdex_client_map_attribute as MapAttributePtr -> MapAttribute #}

mkMapAttribute :: (HyperSerialize k, HyperSerialize v) => ByteString -> k -> v -> MapAttribute
mkMapAttribute name key value =  MapAttribute name (serialize key) (datatype key) (serialize value) (datatype value)
{-# INLINE mkMapAttribute #-}

mkMapAttributesFromMap :: (HyperSerialize k, HyperSerialize v) => ByteString -> (Map k v) -> [MapAttribute]
mkMapAttributesFromMap name = map (uncurry $ mkMapAttribute name) . Map.toList
{-# INLINE mkMapAttributesFromMap #-}

data MapAttribute = MapAttribute
  { mapAttrName      :: ByteString
  , mapAttrKey       :: ByteString
  , mapAttrKeyDatatype   :: Hyperdatatype
  , mapAttrValue     :: ByteString
  , mapAttrValueDatatype :: Hyperdatatype
  }
  deriving (Show, Eq)
instance Storable MapAttribute where
  sizeOf _    = {#sizeof hyperdex_client_map_attribute_struct #}
  alignment _ = {#alignof hyperdex_client_map_attribute_struct #}
  peek p = MapAttribute
    <$> (packCString =<< ({#get hyperdex_client_map_attribute.attr #} p))
    <*> (do
          str <- {#get hyperdex_client_map_attribute.map_key #} p
          len <- {#get hyperdex_client_map_attribute.map_key_sz #} p
          packCStringLen (str, fromIntegral len)
        )
    <*> liftM (toEnum . fromIntegral) ({#get hyperdex_client_map_attribute.map_key_datatype #} p)
    <*> (do
          str <- {#get hyperdex_client_map_attribute.value #} p
          len <- {#get hyperdex_client_map_attribute.value_sz #} p
          packCStringLen (str, fromIntegral len)
        )
    <*> liftM (toEnum . fromIntegral) ({#get hyperdex_client_map_attribute.value_datatype #} p)
  poke p x = do
    attr <- newCBString (mapAttrName x)
    (key, keySize) <- newCBStringLen (mapAttrKey x)
    (value, valueSize) <- newCBStringLen (mapAttrValue x)
    {#set hyperdex_client_map_attribute.attr #} p attr
    {#set hyperdex_client_map_attribute.map_key #} p key
    {#set hyperdex_client_map_attribute.map_key_sz #} p $ (fromIntegral keySize)
    {#set hyperdex_client_map_attribute.map_key_datatype #} p (fromIntegral . fromEnum $ mapAttrKeyDatatype x)
    {#set hyperdex_client_map_attribute.value #} p value
    {#set hyperdex_client_map_attribute.value_sz #} p $ (fromIntegral valueSize)
    {#set hyperdex_client_map_attribute.value_datatype #} p (fromIntegral . fromEnum $ mapAttrValueDatatype x)

rPokeMapAttribute :: MonadResource m => MapAttribute -> Ptr MapAttribute -> m ()
rPokeMapAttribute (MapAttribute {..}) p = do
  name <- rNewCBString0 mapAttrName
  (key, keyLen) <- rNewCBStringLen mapAttrKey
  (value, valueLen) <- rNewCBStringLen mapAttrValue
  let keyType = fromIntegral . fromEnum $ mapAttrKeyDatatype
  let valueType = fromIntegral . fromEnum $ mapAttrValueDatatype
  liftIO $ do
    {#set hyperdex_client_map_attribute.attr #} p name
    {#set hyperdex_client_map_attribute.map_key #} p key
    {#set hyperdex_client_map_attribute.map_key_sz #} p $ fromIntegral keyLen
    {#set hyperdex_client_map_attribute.map_key_datatype #} p keyType
    {#set hyperdex_client_map_attribute.value #} p value
    {#set hyperdex_client_map_attribute.value_sz #} p $ fromIntegral valueLen
    {#set hyperdex_client_map_attribute.value_datatype #} p valueType

rNewMapAttributeArray :: MonadResource m => [MapAttribute] -> m (Ptr MapAttribute, Int)
rNewMapAttributeArray mapAttrs = do
  let len = length mapAttrs
  arrayPtr <- rMallocArray len
  forM_ (zip mapAttrs [0..]) $ \(mapAttr, i) -> do
    let mapAttrPtr = advancePtr arrayPtr i
    rPokeMapAttribute mapAttr mapAttrPtr
  return (arrayPtr, len)
