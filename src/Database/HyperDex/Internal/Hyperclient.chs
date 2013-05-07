
module Database.HyperDex.Internal.Hyperclient where

import Database.HyperDex.Internal.Hyperdex
import Database.HyperDex.Internal.Attribute

import Foreign.C.Types
import Foreign.C.String
import Foreign.Marshal
import Foreign.Ptr
import Foreign.Storable
import Data.Int

import Control.Applicative ((<$>))

#include "hyperclient.h"

data Hyperclient
{#pointer *hyperclient as HyperclientPtr -> Hyperclient #}

{# pointer *hyperclient_attribute as AttributePtr -> Attribute #}

data HyperclientMapAttribute
{#pointer *hyperclient_map_attribute as HyperclientMapAttributePtr -> HyperclientMapAttribute #}

data HyperclientAttributeCheck
{#pointer *hyperclient_attribute_check as HyperclientAttributeCheckPtr -> HyperclientAttributeCheck #}

{#enum hyperclient_returncode as HyperclientReturnCode {underscoreToCase} deriving (Eq, Show) #}

newtype HyperclientHandle = HyperclientHandle { unHyperclientHandle :: Int64 }

toHyperclientHandle :: CLong -> HyperclientHandle
toHyperclientHandle = HyperclientHandle . fromIntegral

-- struct hyperclient*
-- hyperclient_create(const char* coordinator, uint16_t port);
hyperclientCreate :: CString -> Int16 -> IO HyperclientPtr
hyperclientCreate host port = {# call hyperclient_create #} host (fromIntegral port)

-- void
-- hyperclient_destroy(struct hyperclient* client);
hyperclientDestroy :: HyperclientPtr -> IO ()
hyperclientDestroy = {# call hyperclient_destroy #}

-- enum hyperclient_returncode
-- hyperclient_add_space(struct hyperclient* client, const char* description);
hyperclientAddSpace :: HyperclientPtr -> CString -> IO HyperclientReturnCode
hyperclientAddSpace client description = do
  toEnum . fromIntegral <$> {#call hyperclient_add_space #} client description

-- enum hyperclient_returncode
-- hyperclient_rm_space(struct hyperclient* client, const char* space);
hyperclientRemoveSpace :: HyperclientPtr -> CString -> IO HyperclientReturnCode
hyperclientRemoveSpace client space = do
  toEnum . fromIntegral <$> {#call hyperclient_rm_space #} client space

-- int64_t
-- hyperclient_put(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, const struct hyperclient_attribute* attrs,
--                 size_t attrs_sz, enum hyperclient_returncode* status);
hyperclientGet :: HyperclientPtr -> CString -> CString -> Int64
                  -> IO (HyperclientHandle, HyperclientReturnCode, AttributeList) 
hyperclientGet client space key keySize = do
  alloca $ \returnCodePtr ->
   alloca $ \attributePtrPtr ->
    alloca $ \attributeSizePtr -> do
      result <- {#call hyperclient_get#} 
                     client
                     space key (fromIntegral keySize)
                     returnCodePtr attributePtrPtr attributeSizePtr
      returnCode <- toEnum . fromIntegral <$> peek returnCodePtr :: IO HyperclientReturnCode
      attributePtr <- peek attributePtrPtr
      attributeSize <- fromIntegral <$> peek attributeSizePtr 
      attributes <- fromHyperDexAttributeList attributePtr attributeSize
      return (toHyperclientHandle result, returnCode, attributes)

-- int64_t
-- hyperclient_put(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, const struct hyperclient_attribute* attrs,
--                 size_t attrs_sz, enum hyperclient_returncode* status);
hyperclientPut :: HyperclientPtr -> CString -> CString -> Int64
                  -> AttributeList
                  -> IO (HyperclientHandle, HyperclientReturnCode)
hyperclientPut client space key keySize attributes = do
  alloca $ \returnCodePtr -> do
    let attributePtr = attributeListPointer attributes
        attributeSize = attributeListSize attributes
    result <- {# call hyperclient_put #} 
                  client
                  space key (fromIntegral keySize)
                  attributePtr (fromIntegral attributeSize) returnCodePtr
    returnCode <- toEnum . fromIntegral <$> peek returnCodePtr :: IO HyperclientReturnCode
    return (toHyperclientHandle result, returnCode)

-- int64_t
-- hyperclient_put_if_not_exist(struct hyperclient* client, const char* space, const char* key,
--                              size_t key_sz, const struct hyperclient_attribute* attrs,
--                              size_t attrs_sz, enum hyperclient_returncode* status);
hyperclientPutIfNotExist :: HyperclientPtr -> CString -> CString -> Int64
                            -> AttributeList
                            -> IO (HyperclientHandle, HyperclientReturnCode)
hyperclientPutIfNotExist client space key keySize attributes = do
  alloca $ \returnCodePtr -> do
    let attributePtr = attributeListPointer attributes
        attributeSize = attributeListSize attributes
    result <- {# call hyperclient_put_if_not_exist #} 
                  client
                  space key (fromIntegral keySize)
                  attributePtr (fromIntegral attributeSize) returnCodePtr
    returnCode <- toEnum . fromIntegral <$> peek returnCodePtr :: IO HyperclientReturnCode
    return (toHyperclientHandle result, returnCode)

-- int64_t
-- hyperclient_del(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, enum hyperclient_returncode* status);
hyperclientDelete :: HyperclientPtr -> CString -> CString -> Int64 
                     -> IO (HyperclientHandle, HyperclientReturnCode)
hyperclientDelete client space key keySize = do
  alloca $ \returnCodePtr -> do
    result <- {# call hyperclient_del #} 
                  client
                    space key (fromIntegral keySize)
                  returnCodePtr
    returnCode <- toEnum . fromIntegral <$> peek returnCodePtr :: IO HyperclientReturnCode
    return (toHyperclientHandle result, returnCode)
