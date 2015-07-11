{-# LANGUAGE RecordWildCards #-}
-- |
-- Module       : Database.HyperDex.Internal.AttributeCheck
-- Copyright    : (c) Aaron Friel 2013-2014
--                (c) Niklas Hambüchen 2013-2014 
-- License      : BSD-style
-- Maintainer   : mayreply@aaronfriel.com
-- Stability    : unstable
-- Portability  : portable
--
module Database.HyperDex.Internal.AttributeCheck
  ( AttributeCheck (..)
  , AttributeCheckPtr
  , mkAttributeCheck
  , rNewAttributeCheckArray
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
typedef struct hyperdex_client_attribute_check hyperdex_client_attribute_check_struct;
#endc

{# pointer *hyperdex_client_attribute_check as AttributeCheckPtr -> AttributeCheck #}

mkAttributeCheck :: HyperSerialize a => ByteString -> a -> Hyperpredicate -> AttributeCheck
mkAttributeCheck name value predicate =  AttributeCheck name (serialize value) (datatype value) predicate
{-# INLINE mkAttributeCheck #-}

data AttributeCheck = AttributeCheck
  { attrCheckName      :: ByteString
  , attrCheckValue     :: ByteString
  , attrCheckDatatype  :: Hyperdatatype
  , attrCheckPredicate :: Hyperpredicate
  }
  deriving (Show, Eq)

instance Storable AttributeCheck where
  -- Note [sizeOf fudging] in Attribute.chs
  sizeOf _ = 
    case size `mod` align of
          0 -> size
          n -> size + (align - n) 
    where
      align = {#alignof hyperdex_client_attribute_check_struct #}
      size = {#sizeof hyperdex_client_attribute_check_struct #}
  alignment _ = {#alignof hyperdex_client_attribute_check_struct #}
  peek p = AttributeCheck
    <$> (packCString =<< ({#get hyperdex_client_attribute_check.attr #} p))
    <*> (do
          str <- {#get hyperdex_client_attribute_check.value #} p
          len <- {#get hyperdex_client_attribute_check.value_sz #} p
          packCStringLen (str, fromIntegral len)
        )
    <*> liftM (toEnum . fromIntegral) ({#get hyperdex_client_attribute_check.datatype #} p)
    <*> liftM (toEnum . fromIntegral) ({#get hyperdex_client_attribute_check.predicate #} p)
  poke p x = do
    attr <- newCBString (attrCheckName x)
    (value, valueSize) <- newCBStringLen (attrCheckValue x)
    {#set hyperdex_client_attribute_check.attr #} p attr
    {#set hyperdex_client_attribute_check.value #} p value
    {#set hyperdex_client_attribute_check.value_sz #} p $ (fromIntegral valueSize)
    {#set hyperdex_client_attribute_check.datatype #} p (fromIntegral . fromEnum $ attrCheckDatatype x)
    {#set hyperdex_client_attribute_check.predicate #} p (fromIntegral . fromEnum $ attrCheckPredicate x)

rPokeAttributeCheck :: MonadResource m => AttributeCheck -> Ptr AttributeCheck -> m ()
rPokeAttributeCheck (AttributeCheck {..}) p = do
  name <- rNewCBString0 attrCheckName
  (value, valueLen) <- rNewCBStringLen attrCheckValue
  let datatypeVal = fromIntegral . fromEnum $ attrCheckDatatype
  let predicate = fromIntegral . fromEnum $ attrCheckPredicate
  liftIO $ do
    {#set hyperdex_client_attribute_check.attr #} p name
    {#set hyperdex_client_attribute_check.value #} p value
    {#set hyperdex_client_attribute_check.value_sz #} p $ (fromIntegral valueLen)
    {#set hyperdex_client_attribute_check.datatype #} p datatypeVal
    {#set hyperdex_client_attribute_check.predicate #} p predicate

-- rNewAttributeCheck :: MonadResource m => AttributeCheck -> m (Ptr AttributeCheck)
-- rNewAttributeCheck attrCheck = do
--   ptr <- rMalloc
--   rPokeAttributeCheck attrCheck ptr
--   return ptr

rNewAttributeCheckArray :: MonadResource m => [AttributeCheck] -> m (Ptr AttributeCheck, Int)
rNewAttributeCheckArray checks = do
  let len = length checks
  arrayPtr <- rMallocArray len
  forM_ (zip checks [0..]) $ \(attrCheck, i) -> do
    let attrCheckPtr = advancePtr arrayPtr i
    rPokeAttributeCheck attrCheck attrCheckPtr
  return (arrayPtr, len)
