module Database.HyperDex.Internal.Attribute 
  ( fromHyperDexAttributeArray
  , newHyperDexAttributeArray
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

#include "hyperclient.h"

#c
typedef struct hyperclient_attribute hyperclient_attribute_struct;
#endc

{# pointer *hyperclient_attribute as AttributePtr -> Attribute #}

mkAttribute :: HyperSerialize a => ByteString -> a -> Attribute
mkAttribute name value =  Attribute name (serialize value) (datatype value)

fromHyperDexAttributeArray :: Ptr Attribute -> Int -> IO [Attribute]
fromHyperDexAttributeArray p s = peekArray s p

newHyperDexAttributeArray :: [Attribute] -> IO (Ptr Attribute, Int)
newHyperDexAttributeArray as = newArray as >>= \ptr -> return (ptr, length as)

data Attribute = Attribute
  { attrName     :: ByteString
  , attrValue    :: ByteString
  , attrDatatype :: Hyperdatatype
  }
  deriving (Show, Eq, Ord)
instance Storable Attribute where
  sizeOf _ = {#sizeof hyperclient_attribute_struct #}
  alignment _ = {#alignof hyperclient_attribute_struct #}
  peek p = Attribute
    <$> (peekCBString =<< ({#get hyperclient_attribute.attr #} p))
    <*> (do
          str <- {#get hyperclient_attribute.value #} p
          len <- {#get hyperclient_attribute.value_sz #} p
          peekCBStringLen (str, fromIntegral len)
        )
    <*> liftM (toEnum . fromIntegral) ({#get hyperclient_attribute.datatype #} p)
  poke p x = do
    attr <- newCBString (attrName x)
    (value, valueSize) <- newCBStringLen (attrValue x)
    {#set hyperclient_attribute.attr #} p attr
    {#set hyperclient_attribute.value #} p value
    {#set hyperclient_attribute.value_sz #} p $ (fromIntegral valueSize)
    {#set hyperclient_attribute.datatype #} p (fromIntegral . fromEnum $ attrDatatype x)

haskellFreeAttributes :: Ptr Attribute -> Int -> IO ()
haskellFreeAttributes _ 0 = return ()
haskellFreeAttributes p n = do
  free =<< {# get hyperclient_attribute.attr #} p
  free =<< {# get hyperclient_attribute.value #} p
  haskellFreeAttributes p (n-1)

hyperdexFreeAttributes :: Ptr Attribute -> Int -> IO ()
hyperdexFreeAttributes attributes attributeSize =
  {# call hyperclient_destroy_attrs #} attributes (fromIntegral attributeSize)
