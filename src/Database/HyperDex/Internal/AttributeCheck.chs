module Database.HyperDex.Internal.AttributeCheck
  ( AttributeCheck (..)
  , AttributeCheckPtr
  , mkAttributeCheck
  , newHyperDexAttributeCheckArray
  , haskellFreeAttributeChecks
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
typedef struct hyperdex_client_attribute_check hyperdex_client_attribute_check_struct;
#endc

{# pointer *hyperdex_client_attribute_check as AttributeCheckPtr -> AttributeCheck #}

mkAttributeCheck :: HyperSerialize a => ByteString -> a -> Hyperpredicate -> AttributeCheck
mkAttributeCheck name value predicate =  AttributeCheck name (serialize value) (datatype value) predicate
{-# INLINE mkAttributeCheck #-}

newHyperDexAttributeCheckArray :: [AttributeCheck] -> IO (Ptr AttributeCheck, Int)
newHyperDexAttributeCheckArray as = newArray as >>= \ptr -> return (ptr, length as)
{-# INLINE newHyperDexAttributeCheckArray #-}

data AttributeCheck = AttributeCheck
  { attrCheckName      :: ByteString
  , attrCheckValue     :: ByteString
  , attrCheckDatatype  :: Hyperdatatype
  , attrCheckPredicate :: Hyperpredicate
  }
  deriving (Show, Eq)
instance Storable AttributeCheck where
  sizeOf _ = {#sizeof hyperdex_client_attribute_check_struct #}
  alignment _ = {#alignof hyperdex_client_attribute_check_struct #}
  peek p = AttributeCheck
    <$> (peekCBString =<< ({#get hyperdex_client_attribute_check.attr #} p))
    <*> (do
          str <- {#get hyperdex_client_attribute_check.value #} p
          len <- {#get hyperdex_client_attribute_check.value_sz #} p
          peekCBStringLen (str, fromIntegral len)
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

haskellFreeAttributeChecks :: Ptr AttributeCheck -> Int -> IO ()
haskellFreeAttributeChecks _ 0 = return ()
haskellFreeAttributeChecks p n = do
  free =<< {# get hyperdex_client_attribute.attr #} p
  free =<< {# get hyperdex_client_attribute.value #} p
  haskellFreeAttributeChecks p (n-1)
{-# INLINE haskellFreeAttributeChecks #-}
