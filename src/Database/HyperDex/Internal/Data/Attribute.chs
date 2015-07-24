{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
-- |
-- Module       : Database.HyperDex.Internal.Data.Attribute
-- Copyright    : (c) Aaron Friel 2013-2014
--                (c) Niklas Hambüchen 2013-2014 
-- License      : BSD-style
-- Maintainer   : mayreply@aaronfriel.com
-- Stability    : unstable
-- Portability  : portable
--
module Database.HyperDex.Internal.Data.Attribute
  ( Attribute (..)
  , AttributePtr
  , mkAttribute
  , rMallocAttributeArray
  , rPeekAttributeArray
  , rNewAttributeArray
  )
  where

import Data.ByteString (ByteString, empty, packCString, packCStringLen)

import Database.HyperDex.Internal.Serialize
import Database.HyperDex.Internal.Data.Hyperdex
import Database.HyperDex.Internal.Util.Foreign
import Database.HyperDex.Internal.Util.Resource
import Database.HyperDex.Internal.Util

import GHC.Generics (Generic)
import Control.DeepSeq
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
  deriving (Show, Eq, Ord, Generic)
instance NFData (Attribute)
instance Storable Attribute where
  -- Note [sizeOf fudging]
  sizeOf _ = 
    case size `mod` align of
          0 -> size
          n -> size + (align - n) 
    where
      align = {#alignof hyperdex_client_attribute_struct #}
      size = {#sizeof hyperdex_client_attribute_struct #}
  alignment _ = {#alignof hyperdex_client_attribute_struct #}
  peek p = Attribute
    <$> (packCString =<< ({#get hyperdex_client_attribute.attr #} p))
    <*> (do
          str <- {#get hyperdex_client_attribute.value #} p
          len <- {#get hyperdex_client_attribute.value_sz #} p
          case len > 0 of 
            True  -> packCStringLen (str, fromIntegral len)
            False -> return empty
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
rPokeAttribute (Attribute {..}) p = do
  name <- rNewCBString0 attrName
  (value, valueLen) <- rNewCBStringLen attrValue
  let datatypeVal = fromIntegral . fromEnum $ attrDatatype
  liftIO $ do
    {#set hyperdex_client_attribute.attr #} p name
    {#set hyperdex_client_attribute.value #} p value
    {#set hyperdex_client_attribute.value_sz #} p (fromIntegral valueLen)
    {#set hyperdex_client_attribute.datatype #} p datatypeVal
{-# INLINE rPokeAttribute #-}

rMallocAttributeArray :: MonadResource m 
                      => m (Ptr (Ptr Attribute), Ptr CULong, m [Attribute])
rMallocAttributeArray = do
  ptrPtr <- rMalloc
  szPtr <- rMalloc
  let peekFn = rPeekAttributeArray ptrPtr szPtr
  return (ptrPtr, szPtr, peekFn)
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
  forM_ (zip attrs [0..]) $ \(attr, i) -> do
    let attrPtr = advancePtr arrayPtr i
    rPokeAttribute attr attrPtr
  return (arrayPtr, len)
{-# INLINE rNewAttributeArray #-}

hyperdexFreeAttributes :: Ptr Attribute -> CULong -> IO ()
hyperdexFreeAttributes attributes attributeSize = wrapHyperCall $
  {# call hyperdex_client_destroy_attrs #} attributes attributeSize
{-# INLINE hyperdexFreeAttributes #-}

{- Note [Float coercions]
~~~~~~~~~~~~~~~~~~~~~~
On x64 systems, at least, the sizeOf parameter needs to be increased
to a multiple of the alignment. See this memory dump from GDB:

0x7f1e40004830: 0x00007f1e40004a90      0x00007f1e40004a96
0x7f1e40004840: 0x0000000000000000      0x00007f1e00002401
0x7f1e40004850: 0x00007f1e40004a96      0x00007f1e40004a9b
0x7f1e40004860: 0x0000000000000000      0x00007f1e00002401
0x7f1e40004870: 0x00007f1e40004a9b      0x00007f1e40004aa1
0x7f1e40004880: 0x0000000000000000      0x00007f1e00002403
0x7f1e40004890: 0x00007f1e40004aa1      0x00007f1e40004aaf
0x7f1e400048a0: 0x0000000000000000      0x00007f1e00002402

Address 0x7f1e40004830 is the start of an array of hyperdex_client_attribute.

The first 8 bytes are a pointer to a string, the next 8 bytes likewise,
and the value at 0x7f1e40004840 is an 8-byte length (0). Note that at 
address 0x7f1e40004848 there is a 4-byte integer (0x2401) and garbage
in the high bytes. 

If the sizeOf is left at 28, then peekElemOff (used by peekArray) will
use the address 0x7f1e4000484c as the start of the next hyperdex_client_attribute.

This is incorrect, as the next element is at 0x7f1e40004850, or 32 bytes
following the first. 

Solution: fudge the sizeOf so that it is a multiple of the alignment size.
-}
