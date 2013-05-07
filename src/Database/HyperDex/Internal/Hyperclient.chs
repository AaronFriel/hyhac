
module Database.HyperDex.Internal.Hyperclient where

import Database.HyperDex.Internal.Attribute
import Database.HyperDex.Internal.Util

import Foreign.C.Types
import Foreign.Marshal
import Foreign.Ptr
import Foreign.Storable

import Data.ByteString (ByteString)
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
hyperclientCreate :: ByteString -> Int16 -> IO HyperclientPtr
hyperclientCreate h port = withCBString h $ \host -> {# call hyperclient_create #} host (fromIntegral port)

-- void
-- hyperclient_destroy(struct hyperclient* client);
hyperclientDestroy :: HyperclientPtr -> IO ()
hyperclientDestroy = {# call hyperclient_destroy #}

-- enum hyperclient_returncode
-- hyperclient_add_space(struct hyperclient* client, const char* description);
hyperclientAddSpace :: HyperclientPtr -> ByteString -> IO HyperclientReturnCode
hyperclientAddSpace client d = withCBString d $ \description -> do
  toEnum . fromIntegral <$> {#call hyperclient_add_space #} client description

-- enum hyperclient_returncode
-- hyperclient_rm_space(struct hyperclient* client, const char* space);
hyperclientRemoveSpace :: HyperclientPtr -> ByteString -> IO HyperclientReturnCode
hyperclientRemoveSpace client s = withCBString s $ \space -> do
  toEnum . fromIntegral <$> {#call hyperclient_rm_space #} client space

-- int64_t
-- hyperclient_put(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, const struct hyperclient_attribute* attrs,
--                 size_t attrs_sz, enum hyperclient_returncode* status);
hyperclientGet :: HyperclientPtr -> ByteString -> ByteString
                  -> IO (HyperclientHandle, HyperclientReturnCode, AttributeList) 
hyperclientGet client s k = do
  alloca $ \returnCodePtr ->
   alloca $ \attributePtrPtr ->
    alloca $ \attributeSizePtr ->
     withCBString s $ \space ->
      withCBStringLen k $ \(key,keySize) -> do
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
hyperclientPut :: HyperclientPtr -> ByteString -> ByteString
                  -> AttributeList
                  -> IO (HyperclientHandle, HyperclientReturnCode)
hyperclientPut client s k attributes = do
  alloca $ \returnCodePtr ->
   withCBString s $ \space ->
    withCBStringLen k $ \(key,keySize) -> do
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
hyperclientPutIfNotExist :: HyperclientPtr -> ByteString -> ByteString
                            -> AttributeList
                            -> IO (HyperclientHandle, HyperclientReturnCode)
hyperclientPutIfNotExist client s k attributes = do
  alloca $ \returnCodePtr ->
   withCBString s $ \space ->
    withCBStringLen k $ \(key,keySize) -> do
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
hyperclientDelete :: HyperclientPtr -> ByteString -> ByteString
                     -> IO (HyperclientHandle, HyperclientReturnCode)
hyperclientDelete client s k = do
  alloca $ \returnCodePtr -> do
   withCBString s $ \space ->
    withCBStringLen k $ \(key,keySize) -> do
      result <- {# call hyperclient_del #} 
                    client
                      space key (fromIntegral keySize)
                    returnCodePtr
      returnCode <- toEnum . fromIntegral <$> peek returnCodePtr :: IO HyperclientReturnCode
      return (toHyperclientHandle result, returnCode)
