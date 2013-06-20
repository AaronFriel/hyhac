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

#include "hyperclient.h"

#c
typedef struct hyperclient_map_attribute hyperclient_map_attribute_struct;
#endc

{# pointer *hyperclient_map_attribute as MapAttributePtr -> MapAttribute #}

mkMapAttribute :: (HyperSerialize k, HyperSerialize v) => ByteString -> k -> v -> MapAttribute
mkMapAttribute name key value =  MapAttribute name (serialize key) (datatype key) (serialize value) (datatype value)

mkMapAttributesFromMap :: (HyperSerialize k, HyperSerialize v) => ByteString -> (Map k v) -> [MapAttribute]
mkMapAttributesFromMap name = map (uncurry $ mkMapAttribute name) . Map.toList

newHyperDexMapAttributeArray :: [MapAttribute] -> IO (Ptr MapAttribute, Int)
newHyperDexMapAttributeArray as = newArray as >>= \ptr -> return (ptr, length as)

data MapAttribute = MapAttribute
  { mapAttrName      :: ByteString
  , mapAttrKey       :: ByteString
  , mapAttrKeyDatatype   :: Hyperdatatype
  , mapAttrValue     :: ByteString
  , mapAttrValueDatatype :: Hyperdatatype
  }
  deriving (Show, Eq)
instance Storable MapAttribute where
  sizeOf _    = {#sizeof hyperclient_map_attribute_struct #}
  alignment _ = {#alignof hyperclient_map_attribute_struct #}
  peek p = MapAttribute
    <$> (peekCBString =<< ({#get hyperclient_map_attribute.attr #} p))
    <*> (do
          str <- {#get hyperclient_map_attribute.map_key #} p
          len <- {#get hyperclient_map_attribute.map_key_sz #} p
          peekCBStringLen (str, fromIntegral len)
        )
    <*> liftM (toEnum . fromIntegral) ({#get hyperclient_map_attribute.map_key_datatype #} p)
    <*> (do
          str <- {#get hyperclient_map_attribute.value #} p
          len <- {#get hyperclient_map_attribute.value_sz #} p
          peekCBStringLen (str, fromIntegral len)
        )
    <*> liftM (toEnum . fromIntegral) ({#get hyperclient_map_attribute.value_datatype #} p)
  poke p x = do
    attr <- newCBString (mapAttrName x)
    (key, keySize) <- newCBStringLen (mapAttrKey x)
    (value, valueSize) <- newCBStringLen (mapAttrValue x)
    {#set hyperclient_map_attribute.attr #} p attr
    {#set hyperclient_map_attribute.map_key #} p key
    {#set hyperclient_map_attribute.map_key_sz #} p $ (fromIntegral keySize)
    {#set hyperclient_map_attribute.map_key_datatype #} p (fromIntegral . fromEnum $ mapAttrKeyDatatype x)
    {#set hyperclient_map_attribute.value #} p value
    {#set hyperclient_map_attribute.value_sz #} p $ (fromIntegral valueSize)
    {#set hyperclient_map_attribute.value_datatype #} p (fromIntegral . fromEnum $ mapAttrValueDatatype x)
    
haskellFreeMapAttributes :: Ptr MapAttribute -> Int -> IO ()
haskellFreeMapAttributes _ 0 = return ()
haskellFreeMapAttributes p n = do
  free =<< {# get hyperclient_attribute.attr #} p
  free =<< {# get hyperclient_attribute.value #} p
  haskellFreeMapAttributes p (n-1)
