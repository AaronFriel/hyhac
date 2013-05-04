module Database.HyperDex.Internal.Hyperclient where

import Foreign.C.Types
import Foreign.C.String
-- import Foreign.Marshal.Utils
import Foreign.Marshal.Alloc
import Foreign.Ptr
import Foreign.Storable
import Data.Int

import Control.Applicative ((<$>))

#import "hyperclient.h"

data Hyperclient
{#pointer *hyperclient as HyperclientPtr -> Hyperclient #}

data HyperclientAttribute
{#pointer *hyperclient_attribute as HyperclientAttributePtr -> HyperclientAttribute #}

data HyperclientMapAttribute
{#pointer *hyperclient_map_attribute as HyperclientMapAttributePtr -> HyperclientMapAttribute #}

data HyperclientAttributeCheck
{#pointer *hyperclient_attribute_check as HyperclientAttributeCheckPtr -> HyperclientAttributeCheck #}

{#enum hyperclient_returncode as HyperclientReturnCode {underscoreToCase} deriving (Eq, Show) #}

-- struct hyperclient*
-- hyperclient_create(const char* coordinator, uint16_t port);
hyperclientCreate :: String -> Int16 -> IO HyperclientPtr
hyperclientCreate host port = do
  inHost <- newCString host
  {# call hyperclient_create #} inHost (fromIntegral port)

-- void
-- hyperclient_destroy(struct hyperclient* client);
hyperclientDestroy :: HyperclientPtr -> IO ()
hyperclientDestroy client = {# call hyperclient_destroy #} client

-- enum hyperclient_returncode
-- hyperclient_add_space(struct hyperclient* client, const char* description);
hyperclientAddSpace :: HyperclientPtr -> String -> IO HyperclientReturnCode
hyperclientAddSpace client description = do
  inDescription <- newCString description
  toEnum . fromIntegral <$>
    {#call hyperclient_add_space #} client inDescription

-- enum hyperclient_returncode
-- hyperclient_rm_space(struct hyperclient* client, const char* space);
hyperclientRemoveSpace :: HyperclientPtr -> String -> IO HyperclientReturnCode
hyperclientRemoveSpace client space = do
  inSpace <- newCString space
  toEnum . fromIntegral <$>
    {#call hyperclient_rm_space #} client inSpace

-- int64_t
-- hyperclient_put(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, const struct hyperclient_attribute* attrs,
--                 size_t attrs_sz, enum hyperclient_returncode* status);
hyperclientGet :: HyperclientPtr -> String -> String -> IO (Int64, HyperclientReturnCode, HyperclientAttributePtr, Int64) 
hyperclientGet client space key = do
  returnCodePtr <- malloc 
  attributePtrPtr <- malloc 
  attributeSizePtr <- malloc
  (inSpace) <- newCString space
  (inKey, inKeySize) <- newCStringLen key
  result <- {#call hyperclient_get#} 
                client
                inSpace inKey (fromIntegral inKeySize)
                returnCodePtr attributePtrPtr attributeSizePtr
  returnCode <- toEnum . fromIntegral <$> peek returnCodePtr :: IO HyperclientReturnCode
  attributePtr <- peek attributePtrPtr
  attributeSize <- fromIntegral <$> peek attributeSizePtr :: IO Int64
  return (fromIntegral result, returnCode, attributePtr, attributeSize)

-- int64_t
-- hyperclient_put(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, const struct hyperclient_attribute* attrs,
--                 size_t attrs_sz, enum hyperclient_returncode* status);
hyperclientPut :: HyperclientPtr -> String -> String -> HyperclientAttributePtr -> Int64 -> IO (Int64, HyperclientReturnCode)
hyperclientPut client space key attributes attributeSize = do
  returnCodePtr <- malloc
  (inSpace) <- newCString space
  (inKey, inKeySize) <- newCStringLen key
  result <- {# call hyperclient_put #} 
                client
                inSpace inKey (fromIntegral inKeySize)
                attributes (fromIntegral attributeSize) returnCodePtr
  returnCode <- toEnum . fromIntegral <$> peek returnCodePtr :: IO HyperclientReturnCode
  return (fromIntegral result, returnCode)

-- int64_t
-- hyperclient_put_if_not_exist(struct hyperclient* client, const char* space, const char* key,
--                              size_t key_sz, const struct hyperclient_attribute* attrs,
--                              size_t attrs_sz, enum hyperclient_returncode* status);
hyperclientPutIfNotExist :: HyperclientPtr -> String -> String -> HyperclientAttributePtr -> Int64 -> IO (Int64, HyperclientReturnCode)
hyperclientPutIfNotExist client space key attributes attributeSize = do
  returnCodePtr <- malloc
  (inSpace) <- newCString space
  (inKey, inKeySize) <- newCStringLen key
  result <- {# call hyperclient_put_if_not_exist #} 
                client
                inSpace inKey (fromIntegral inKeySize)
                attributes (fromIntegral attributeSize) returnCodePtr
  returnCode <- toEnum . fromIntegral <$> peek returnCodePtr :: IO HyperclientReturnCode
  return (fromIntegral result, returnCode)

-- int64_t
-- hyperclient_del(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, enum hyperclient_returncode* status);
hyperclientDelete :: HyperclientPtr -> String -> String -> IO (Int64, HyperclientReturnCode)
hyperclientDelete client space key = do
  returnCodePtr <- malloc
  (inSpace) <- newCString space
  (inKey, inKeySize) <- newCStringLen key
  result <- {# call hyperclient_del #} 
                client
                inSpace inKey (fromIntegral inKeySize)
                returnCodePtr
  returnCode <- toEnum . fromIntegral <$> peek returnCodePtr :: IO HyperclientReturnCode
  return (fromIntegral result, returnCode)

hyperclientDestroyAttributes :: HyperclientAttributePtr -> Int64 -> IO ()
hyperclientDestroyAttributes attributes attributeSize =
  {# call hyperclient_destroy_attrs #} attributes (fromIntegral attributeSize)
