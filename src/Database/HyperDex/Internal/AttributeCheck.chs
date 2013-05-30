module Database.HyperDex.Internal.AttributeCheck
  ( AttributeCheck (..)
  , AttributeCheckPtr
  )
  where

import Database.HyperDex.Internal.Hyperdex
import Database.HyperDex.Internal.Hyperdata
import Database.HyperDex.Internal.Util

import Control.Monad
import Control.Applicative ((<$>), (<*>))

#include "hyperclient.h"

#c
typedef struct hyperclient_attribute_check hyperclient_attribute_check_struct;
#endc

{# pointer *hyperclient_attribute_check as AttributeCheckPtr -> AttributeCheck #}

data AttributeCheck = AttributeCheck
  { attrCheckName      :: ByteString
  , attrCheckValue     :: ByteString
  , attrCheckDatatype  :: Hyperdatatype
  , attrCheckPredicate :: Hyperpredicate
  }
  deriving (Show, Eq)
instance Storable AttributeCheck where
  sizeOf _ = {#sizeof hyperclient_attribute_check_struct #}
  alignment _ = {#alignof hyperclient_attribute_check_struct #}
  peek p = AttributeCheck
    <$> (peekCBString =<< ({#get hyperclient_attribute_check.attr #} p))
    <*> (do
          str <- {#get hyperclient_attribute_check.value #} p
          len <- {#get hyperclient_attribute_check.value_sz #} p
          peekCBStringLen (str, fromIntegral len)
        )
    <*> liftM (toEnum . fromIntegral) ({#get hyperclient_attribute_check.datatype #} p)
    <*> liftM (toEnum . fromIntegral) ({#get hyperclient_attribute_check.predicate #} p)
  poke p x = do
    attr <- newCBString (attrCheckName x)
    (value, valueSize) <- newCBStringLen (attrCheckValue x)
    {#set hyperclient_attribute_check.attr #} p attr
    {#set hyperclient_attribute_check.value #} p value
    {#set hyperclient_attribute_check.value_sz #} p $ (fromIntegral valueSize)
    {#set hyperclient_attribute_check.datatype #} p (fromIntegral . fromEnum $ attrCheckDatatype x)
    {#set hyperclient_attribute_check.predicate #} p (fromIntegral . fromEnum $ attrCheckPredicate x)
