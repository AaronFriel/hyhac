module Database.HyperDex.Internal.MapAttribute
  ( MapAttribute (..)
  , MapAttributePtr
  , mkMapAttribute
  , mkMapAttributesFromMap
  , newHyperDexMapAttributeArray
  , haskellFreeMapAttributes
  )
  where

import Foreign
import Foreign.C

import Data.ByteString (ByteString)

import Database.HyperDex.Internal.Hyperdex
import Database.HyperDex.Internal.Hyperdata
import Database.HyperDex.Internal.Util

import Data.Map (Map)
import qualified Data.Map as Map

import Control.Monad
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

newHyperDexMapAttributeArray :: [MapAttribute] -> IO (Ptr MapAttribute, Int)
newHyperDexMapAttributeArray as = newArray as >>= \ptr -> return (ptr, length as)
{-# INLINE newHyperDexMapAttributeArray #-}

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
    <$> (peekCBString =<< ({#get hyperdex_client_map_attribute.attr #} p))
    <*> (do
          str <- {#get hyperdex_client_map_attribute.map_key #} p
          len <- {#get hyperdex_client_map_attribute.map_key_sz #} p
          peekCBStringLen (str, fromIntegral len)
        )
    <*> liftM (toEnum . fromIntegral) ({#get hyperdex_client_map_attribute.map_key_datatype #} p)
    <*> (do
          str <- {#get hyperdex_client_map_attribute.value #} p
          len <- {#get hyperdex_client_map_attribute.value_sz #} p
          peekCBStringLen (str, fromIntegral len)
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

haskellFreeMapAttributes :: Ptr MapAttribute -> Int -> IO ()
haskellFreeMapAttributes _ 0 = return ()
haskellFreeMapAttributes p n = do
  free =<< {# get hyperdex_client_attribute.attr #} p
  free =<< {# get hyperdex_client_attribute.value #} p
  haskellFreeMapAttributes p (n-1)
{-# INLINE haskellFreeMapAttributes #-}

