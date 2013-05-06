module Database.HyperDex.Internal.Attribute where

import Database.HyperDex.Internal.Hyperdex
import Database.HyperDex.Internal.Hyperclient

import Foreign.C.Types
import Foreign.C.String
import Foreign.Ptr
import Foreign.Storable

import Data.ByteString

import Control.Monad
import Control.Applicative ((<$>), (<*>))

#include "hyperclient.h"

#c
typedef struct hyperclient_attribute hyperclient_attribute;
#endc

data Attribute = Attribute
  { attr'Attribute :: CString
  , value'Attribute :: CString
  , value_sz'Attribute :: CULong
  , datatype'Attribute :: Hyperdatatype
  }
instance Storable Attribute where
  sizeOf _ = {#sizeof hyperclient_attribute#}
  alignment _ = {#alignof hyperclient_attribute#}
  peek p = Attribute
    <$> ({#get hyperclient_attribute.attr #} p)
    <*> ({#get hyperclient_attribute.value #} p)
    <*> ({#get hyperclient_attribute.value_sz #} p)
    <*> liftM (toEnum . fromIntegral) ({#get hyperclient_attribute.datatype #} p)
  poke p x = do
    {#set hyperclient_attribute.attr #} p (attr'Attribute x)
    {#set hyperclient_attribute.value #} p (value'Attribute x)
    {#set hyperclient_attribute.value_sz #} p $ (value_sz'Attribute x)
    {#set hyperclient_attribute.datatype #} p (fromIntegral . fromEnum $ datatype'Attribute x)
