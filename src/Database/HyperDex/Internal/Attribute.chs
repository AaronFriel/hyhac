module Database.HyperDex.Internal.Attribute 
  ( AttributeList (..)
  , fromHyperDexAttributeList
  , fromHaskellAttributeList
  , attributeListPointer
  , attributeListSize
  , Attribute (..)
  , hyperclientDestroyAttributes
  , freeAttributeList
  )
  where

import Database.HyperDex.Internal.Hyperdex
import Database.HyperDex.Internal.Util

import Data.ByteString (ByteString)

import Foreign.C
import Foreign.Marshal
import Foreign.Ptr
import Foreign.Storable

import Control.Monad
import Control.Applicative ((<$>), (<*>))

#include "hyperclient.h"

#c
typedef struct hyperclient_attribute hyperclient_attribute_struct;
#endc

{# pointer *hyperclient_attribute as AttributePtr -> Attribute #}

newtype AttributeList = 
  AttributeList { unAttributeList :: ([Attribute], Ptr Attribute, Int, AllocBy) }

fromHyperDexAttributeList :: Ptr Attribute -> Int -> IO AttributeList
fromHyperDexAttributeList p s = do
  list <- peekArray s p
  return $ AttributeList (list, p, s, AllocHyperDex)

fromHaskellAttributeList :: [Attribute] -> IO AttributeList
fromHaskellAttributeList attributes = do
  let s = length attributes
  array <- newArray attributes
  return $ AttributeList (attributes, array, s, AllocHaskell)

attributeListPointer :: AttributeList -> Ptr Attribute
attributeListPointer (AttributeList (_,p,_,_)) = p

attributeListSize :: AttributeList -> Int
attributeListSize (AttributeList (_,_,s,_)) = s

data Attribute = Attribute
  { attr'Attribute      :: ByteString
  , value'Attribute     :: ByteString
  , datatype'Attribute  :: Hyperdatatype
  }
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
    attr <- newCBString (attr'Attribute x)
    (value, valueSize) <- newCBStringLen (value'Attribute x)
    {#set hyperclient_attribute.attr #} p attr
    {#set hyperclient_attribute.value #} p (value)
    {#set hyperclient_attribute.value_sz #} p $ (fromIntegral valueSize)
    {#set hyperclient_attribute.datatype #} p (fromIntegral . fromEnum $ datatype'Attribute x)

hyperclientDestroyAttributes :: Ptr Attribute -> Int -> IO ()
hyperclientDestroyAttributes attributes attributeSize =
  {# call hyperclient_destroy_attrs #} attributes (fromIntegral attributeSize)

freeAttributeList :: AttributeList -> IO ()
freeAttributeList attr =
  case unAttributeList attr of
    (_,p,s,AllocHaskell) -> freeGHCAlloc p s >> free p
    (_,p,s,AllocHyperDex) -> hyperclientDestroyAttributes p s
  where
    freeGHCAlloc :: Ptr Attribute -> Int -> IO ()
    freeGHCAlloc _ 0 = return ()
    freeGHCAlloc p n = do
      free =<< {# get hyperclient_attribute.attr #} p
      free =<< {# get hyperclient_attribute.value #} p
      freeGHCAlloc p (n-1)
