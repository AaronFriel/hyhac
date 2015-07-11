module Database.HyperDex.Internal.Attribute
  ( newHyperDexAttributeArray
  , Attribute (..)
  , AttributePtr
  , mkAttribute
  , haskellFreeAttributes
  , hyperdexFreeAttributes
  )
  where

import Foreign
import Foreign.C

import Data.ByteString (ByteString, empty, packCString, packCStringLen)

import Database.HyperDex.Internal.Hyperdex
import Database.HyperDex.Internal.Hyperdata
import Database.HyperDex.Internal.Util

import Control.Monad
import Control.Applicative ((<$>), (<*>))

#include "hyperdex/client.h"

#c
typedef struct hyperdex_client_attribute hyperdex_client_attribute_struct;
#endc

{# pointer *hyperdex_client_attribute as AttributePtr -> Attribute #}

mkAttribute :: HyperSerialize a => ByteString -> a -> Attribute
mkAttribute name value =  Attribute name (serialize value) (datatype value)
{-# INLINE mkAttribute #-}

newHyperDexAttributeArray :: [Attribute] -> IO (Ptr Attribute, Int)
newHyperDexAttributeArray as = newArray as >>= \ptr -> return (ptr, length as)
{-# INLINE newHyperDexAttributeArray #-}

data Attribute = Attribute
  { attrName     :: ByteString
  , attrValue    :: ByteString
  , attrDatatype :: Hyperdatatype
  }
  deriving (Show, Eq, Ord)
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

haskellFreeAttributes :: Ptr Attribute -> Int -> IO ()
haskellFreeAttributes _ 0 = return ()
haskellFreeAttributes p n = do
  free =<< {# get hyperdex_client_attribute.attr #} p
  free =<< {# get hyperdex_client_attribute.value #} p
  haskellFreeAttributes p (n-1)
{-# INLINE haskellFreeAttributes #-}

hyperdexFreeAttributes :: Ptr Attribute -> Int -> IO ()
hyperdexFreeAttributes attributes attributeSize = wrapHyperCall $
  {# call hyperdex_client_destroy_attrs #} attributes (fromIntegral attributeSize)
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