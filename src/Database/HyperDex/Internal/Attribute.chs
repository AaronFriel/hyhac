{-# LANGUAGE RecordWildCards #-}
-- |
-- Module       : Database.HyperDex.Internal.Attribute
-- Copyright    : (c) Aaron Friel 2013-2014
--                (c) Niklas Hambüchen 2013-2014 
-- License      : BSD-style
-- Maintainer   : mayreply@aaronfriel.com
-- Stability    : unstable
-- Portability  : portable
--
module Database.HyperDex.Internal.Attribute
  ( Attribute (..)
  , AttributePtr
  , mkAttribute
  , rMallocAttributeArray
  , rPeekAttributeArray
  , rNewAttributeArray
  )
  where

import Foreign
import Foreign.C

import Data.ByteString (ByteString, packCString, packCStringLen)

import Database.HyperDex.Internal.Hyperdex
import Database.HyperDex.Internal.Hyperdata
import Database.HyperDex.Internal.Util

import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import Control.Applicative ((<$>), (<*>))

#include "hyperdex/client.h"

#c
typedef struct hyperdex_client_attribute hyperdex_client_attribute_struct;
#endc

{# pointer *hyperdex_client_attribute as AttributePtr -> Attribute #}

mkAttribute :: HyperSerialize a => ByteString -> a -> Attribute
mkAttribute name value =  Attribute name (serialize value) (datatype value)
{-# INLINE mkAttribute #-}

data Attribute = Attribute
  { attrName     :: ByteString
  , attrValue    :: ByteString
  , attrDatatype :: Hyperdatatype
  }
  deriving (Show, Eq, Ord)
instance Storable Attribute where
  sizeOf _ = {#sizeof hyperdex_client_attribute_struct #}
  alignment _ = {#alignof hyperdex_client_attribute_struct #}
  peek p = Attribute
    <$> (packCString =<< ({#get hyperdex_client_attribute.attr #} p))
    <*> (do
          str <- {#get hyperdex_client_attribute.value #} p
          len <- {#get hyperdex_client_attribute.value_sz #} p
          packCStringLen (str, fromIntegral len)
        )
    <*> liftM (toEnum . fromIntegral) ({#get hyperdex_client_attribute.datatype #} p)
  poke p x = do
    attr <- newCBString (attrName x)
    (value, valueSize) <- newCBStringLen (attrValue x)
    {#set hyperdex_client_attribute.attr #} p attr
    {#set hyperdex_client_attribute.value #} p value
    {#set hyperdex_client_attribute.value_sz #} p $ (fromIntegral valueSize)
    {#set hyperdex_client_attribute.datatype #} p (fromIntegral . fromEnum $ attrDatatype x)

rPokeAttribute :: MonadResource m => Attribute -> Ptr Attribute -> m ()
rPokeAttribute (Attribute {..}) ptr = do
  name <- rNewCBString0 attrName
  (value, valueLen) <- rNewCBStringLen attrValue
  let datatype = fromIntegral . fromEnum $ attrDatatype
  liftIO $ do
    {#set hyperdex_client_attribute.attr #} ptr name
    {#set hyperdex_client_attribute.value #} ptr value
    {#set hyperdex_client_attribute.value_sz #} ptr (fromIntegral valueLen)
    {#set hyperdex_client_attribute.datatype #} ptr datatype
{-# INLINE rPokeAttribute #-}

rMallocAttributeArray :: MonadResource m 
                      => m (Ptr (Ptr Attribute), Ptr CULong, m [Attribute])
rMallocAttributeArray = do
  ptrPtr <- rMalloc
  szPtr <- rMalloc
  let peek = rPeekAttributeArray ptrPtr szPtr
  return (ptrPtr, szPtr, peek)
{-# INLINE rMallocAttributeArray #-}

rPeekAttributeArray :: MonadResource m 
                    => Ptr (Ptr Attribute) 
                    -> Ptr CULong
                    -> m [Attribute]
rPeekAttributeArray ptrPtr szPtr = do
  sz <- liftIO $ peek szPtr
  attrPtr <- liftIO $ peek ptrPtr
  rkey <- register $ hyperdexFreeAttributes attrPtr sz
  attrs <- liftIO $ peekArray (fromIntegral sz) attrPtr
  release rkey
  return attrs
{-# INLINE rPeekAttributeArray #-}

-- rNewAttribute :: MonadResource m => Attribute -> m (Ptr Attribute)
-- rNewAttribute attr = do
--   ptr <- rMalloc
--   rPokeAttribute attr ptr
--   return ptr

rNewAttributeArray :: MonadResource m => [Attribute] -> m (Ptr Attribute, Int)
rNewAttributeArray attrs = do
  let len = length attrs
  arrayPtr <- rMallocArray len
  forM (zip attrs [0..]) $ \(attr, i) -> do
    let attrPtr = advancePtr arrayPtr i
    rPokeAttribute attr attrPtr
  return (arrayPtr, len)
{-# INLINE rNewAttributeArray #-}

hyperdexFreeAttributes :: Ptr Attribute -> CULong -> IO ()
hyperdexFreeAttributes attributes attributeSize = wrapHyperCall $
  {# call hyperdex_client_destroy_attrs #} attributes attributeSize
{-# INLINE hyperdexFreeAttributes #-}
