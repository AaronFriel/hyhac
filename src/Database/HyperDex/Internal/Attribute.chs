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

import Data.ByteString (ByteString)

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
  sizeOf _ = {#sizeof hyperdex_client_attribute_struct #}
  alignment _ = {#alignof hyperdex_client_attribute_struct #}
  peek p = Attribute
    <$> (peekCBString =<< ({#get hyperdex_client_attribute.attr #} p))
    <*> (do
          str <- {#get hyperdex_client_attribute.value #} p
          len <- {#get hyperdex_client_attribute.value_sz #} p
          peekCBStringLen (str, fromIntegral len)
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
