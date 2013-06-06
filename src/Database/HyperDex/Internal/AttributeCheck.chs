module Database.HyperDex.Internal.AttributeCheck
  ( AttributeCheck (..)
  , AttributeCheckPtr
  , mkAttributeCheck
  , newHyperDexAttributeCheckArray
  , haskellFreeAttributeChecks
  )
  where

import Database.HyperDex.Internal.Hyperdex
import Database.HyperDex.Internal.Hyperdata
import Database.HyperDex.Internal.Util

import Control.Monad
import Control.Applicative ((<$>), (<*>))

import Debug.Trace

#include "hyperclient.h"

#c
typedef struct hyperclient_attribute_check hyperclient_attribute_check_struct;
#endc

{# pointer *hyperclient_attribute_check as AttributeCheckPtr -> AttributeCheck #}

mkAttributeCheck :: HyperSerialize a => ByteString -> a -> Hyperpredicate -> AttributeCheck
mkAttributeCheck name value predicate =  AttributeCheck name (serialize value) (datatype value) predicate

newHyperDexAttributeCheckArray :: [AttributeCheck] -> IO (Ptr AttributeCheck, Int)
newHyperDexAttributeCheckArray as = newArray as >>= \ptr -> return (ptr, length as)

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
    traceIO $ "In AttributeCheck poke"
    attr <- newCBString (attrCheckName x)
    traceIO $ "Allocated attribute name ["++ show attr ++"] from attrCheckName ["++ show (attrCheckName x) ++"]"
    traceIO $ "About to allocate attribute value ["++show (attrCheckValue x)++"]"
    (value, valueSize) <- newCBStringLen (attrCheckValue x)
    traceIO $ "Allocated attribute value ["++ show value ++"], valueSize ["++ show valueSize ++"] from attrCheckValue ["++ show (attrCheckValue x) ++"]"
    {#set hyperclient_attribute_check.attr #} p attr
    traceIO $ " - poked attr"
    {#set hyperclient_attribute_check.value #} p value
    traceIO $ " - poked value"
    {#set hyperclient_attribute_check.value_sz #} p $ (fromIntegral valueSize)
    traceIO $ " - poked valueSize"
    {#set hyperclient_attribute_check.datatype #} p (fromIntegral . fromEnum $ attrCheckDatatype x)
    traceIO $ " - poked datatype"
    {#set hyperclient_attribute_check.predicate #} p (fromIntegral . fromEnum $ attrCheckPredicate x)
    traceIO $ " - poked predicate"

haskellFreeAttributeChecks :: Ptr AttributeCheck -> Int -> IO ()
haskellFreeAttributeChecks _ 0 = return ()
haskellFreeAttributeChecks p n = do
  free =<< {# get hyperclient_attribute.attr #} p
  free =<< {# get hyperclient_attribute.value #} p
  haskellFreeAttributeChecks p (n-1)
