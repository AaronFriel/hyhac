module Database.HyperDex.Internal.Hyperclient
  ( hyperGet
  , hyperPut
  , hyperPutIfNotExist
  , hyperDelete 
  )
  where

{# import Database.HyperDex.Internal.ReturnCode #}
{# import Database.HyperDex.Internal.Client #}
{# import Database.HyperDex.Internal.Attribute #}
import Database.HyperDex.Internal.Util

#include "hyperclient.h"

data HyperclientMapAttribute
{#pointer *hyperclient_map_attribute -> HyperclientMapAttribute nocode #}

data HyperclientAttributeCheck
{#pointer *hyperclient_attribute_check -> HyperclientAttributeCheck nocode #}

hyperGet :: Client -> ByteString -> ByteString
            -> IO (IO (HyperclientReturnCode, [Attribute]))
hyperGet c s k = withClient c (\hc -> hyperclientGet hc s k)

hyperPut :: Client -> ByteString -> ByteString -> [Attribute]
            -> IO (IO (HyperclientReturnCode))
hyperPut c s k a = withClient c (\hc -> hyperclientPut hc s k a)

hyperPutIfNotExist :: Client -> ByteString -> ByteString -> [Attribute]
            -> IO (IO (HyperclientReturnCode))
hyperPutIfNotExist c s k a = withClient c (\hc -> hyperclientPutIfNotExist hc s k a)

hyperDelete :: Client -> ByteString -> ByteString
                     -> IO (IO (HyperclientReturnCode))
hyperDelete c s k = withClient c (\hc -> hyperclientDelete hc s k)


-- int64_t
-- hyperclient_put(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, const struct hyperclient_attribute* attrs,
--                 size_t attrs_sz, enum hyperclient_returncode* status);
hyperclientGet :: Hyperclient -> ByteString -> ByteString
                  -> Result (HyperclientReturnCode, [Attribute]) 
hyperclientGet client s k = do
  returnCodePtr <- malloc
  attributePtrPtr <- malloc
  attributeSizePtr <- malloc
  space <- newCBString s
  (key,keySize) <- newCBStringLen k
  handle <- {# call hyperclient_get #}
              client
              space key (fromIntegral keySize)
              returnCodePtr attributePtrPtr attributeSizePtr
  let result :: IO (HyperclientReturnCode, [Attribute])
      result = do
        print "Inside get!"
        returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
        print $ "Got a returnCode!" ++ show returnCode
        attributePtr <- peek attributePtrPtr
        attributeSize <- fmap fromIntegral $ peek attributeSizePtr
        attributes <- fromHyperDexAttributeArray attributePtr attributeSize
        free returnCodePtr
        free attributePtrPtr
        free attributeSizePtr
        free space
        free key
        return (returnCode, attributes)
  return (handle, result)

-- int64_t
-- hyperclient_put(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, const struct hyperclient_attribute* attrs,
--                 size_t attrs_sz, enum hyperclient_returncode* status);
hyperclientPut :: Hyperclient -> ByteString -> ByteString
                  -> [Attribute]
                  -> Result (HyperclientReturnCode)
hyperclientPut client s k attributes = do
  returnCodePtr <- malloc
  space <- newCBString s
  (key,keySize) <- newCBStringLen k
  (attributePtr, attributeSize) <- newHyperDexAttributeArray attributes
  handle <- {# call hyperclient_put #} 
              client
              space key (fromIntegral keySize)
              attributePtr (fromIntegral attributeSize) returnCodePtr
  let continuation = do
        returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
        free returnCodePtr
        free space
        free key
        hyperdexFreeAttributes attributePtr attributeSize
        return returnCode
  return (handle, continuation)

-- int64_t
-- hyperclient_put_if_not_exist(struct hyperclient* client, const char* space, const char* key,
--                              size_t key_sz, const struct hyperclient_attribute* attrs,
--                              size_t attrs_sz, enum hyperclient_returncode* status);
hyperclientPutIfNotExist :: Hyperclient -> ByteString -> ByteString
                            -> [Attribute]
                            -> Result (HyperclientReturnCode)
hyperclientPutIfNotExist client s k attributes = do
  returnCodePtr <- malloc
  space <- newCBString s
  (key,keySize) <- newCBStringLen k
  (attributePtr, attributeSize) <- newHyperDexAttributeArray attributes
  handle <- {# call hyperclient_put_if_not_exist #} 
              client
              space key (fromIntegral keySize)
              attributePtr (fromIntegral attributeSize) returnCodePtr
  let continuation = do
        returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
        free returnCodePtr
        free space
        free key
        hyperdexFreeAttributes attributePtr attributeSize
        return returnCode
  return (handle, continuation)

-- int64_t
-- hyperclient_del(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, enum hyperclient_returncode* status);
hyperclientDelete :: Hyperclient -> ByteString -> ByteString
                     -> Result (HyperclientReturnCode)
hyperclientDelete client s k = do
  returnCodePtr <- malloc
  space <- newCBString s
  (key,keySize) <- newCBStringLen k
  handle <- {# call hyperclient_del #} 
              client
              space key (fromIntegral keySize)
              returnCodePtr
  let continuation = do
        returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
        free returnCodePtr
        free space
        free key
        return returnCode
  return (handle, continuation)
