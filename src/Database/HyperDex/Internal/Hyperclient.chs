module Database.HyperDex.Internal.Hyperclient
  ( hyperGet
  , hyperPut
  , hyperPutIfNotExist
  , hyperDelete 
  , hyperPutConditionally
  )
  where

{# import Database.HyperDex.Internal.ReturnCode #}
{# import Database.HyperDex.Internal.Client #}
{# import Database.HyperDex.Internal.Attribute #}
{# import Database.HyperDex.Internal.AttributeCheck #}
import Database.HyperDex.Internal.Util

import Debug.Trace

#include "hyperclient.h"

data HyperclientMapAttribute
{#pointer *hyperclient_map_attribute -> HyperclientMapAttribute nocode #}

hyperGet :: Client -> ByteString -> ByteString
            -> AsyncResult [Attribute]
hyperGet c s k = withClient c (\hc -> hyperclientGet hc s k)

hyperPut :: Client -> ByteString -> ByteString -> [Attribute]
            -> AsyncResult ()
hyperPut c s k attrs = withClient c (\hc -> hyperclientPut hc s k attrs)

hyperPutIfNotExist :: Client -> ByteString -> ByteString -> [Attribute]
                      -> AsyncResult ()
hyperPutIfNotExist c s k attrs = withClient c (\hc -> hyperclientPutIfNotExist hc s k attrs)

hyperDelete :: Client -> ByteString -> ByteString
               -> AsyncResult ()
hyperDelete c s k = withClient c (\hc -> hyperclientDelete hc s k)

hyperPutConditionally :: Client -> ByteString -> ByteString
                         -> [AttributeCheck] -> [Attribute]
                         -> AsyncResult ()
hyperPutConditionally c s k checks attrs = withClient c (\hc -> hyperclientConditionalPut hc s k checks attrs)

-- int64_t
-- hyperclient_put(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, const struct hyperclient_attribute* attrs,
--                 size_t attrs_sz, enum hyperclient_returncode* status);
hyperclientGet :: Hyperclient -> ByteString -> ByteString
                  -> AsyncResultHandle [Attribute] 
hyperclientGet client s k = do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
  attributePtrPtr <- malloc
  attributeSizePtr <- malloc
  space <- newCBString s
  (key,keySize) <- newCBStringLen k
  handle <- {# call hyperclient_get #}
              client
              space key (fromIntegral keySize)
              returnCodePtr attributePtrPtr attributeSizePtr
  let continuation = do
        returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
        attributes <-
          case returnCode of
            HyperclientSuccess -> do
              attributePtr <- peek attributePtrPtr
              attributeSize <- fmap fromIntegral $ peek attributeSizePtr
              attrs <- fromHyperDexAttributeArray attributePtr attributeSize
              hyperdexFreeAttributes attributePtr attributeSize
              return attrs
            _ -> return []
        free returnCodePtr
        free attributePtrPtr
        free attributeSizePtr
        free space
        free key
        return $ 
          case returnCode of 
            HyperclientSuccess -> Right attributes
            _                  -> Left returnCode
  return (handle, continuation)

-- int64_t
-- hyperclient_put(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, const struct hyperclient_attribute* attrs,
--                 size_t attrs_sz, enum hyperclient_returncode* status);
hyperclientPut :: Hyperclient -> ByteString -> ByteString
                  -> [Attribute]
                  -> AsyncResultHandle ()
hyperclientPut client s k attributes = do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
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
        return $ 
          case returnCode of 
            HyperclientSuccess -> Right ()
            _                  -> Left returnCode
  return (handle, continuation)

-- int64_t
-- hyperclient_put_if_not_exist(struct hyperclient* client, const char* space, const char* key,
--                              size_t key_sz, const struct hyperclient_attribute* attrs,
--                              size_t attrs_sz, enum hyperclient_returncode* status);
hyperclientPutIfNotExist :: Hyperclient -> ByteString -> ByteString
                            -> [Attribute]
                            -> AsyncResultHandle ()
hyperclientPutIfNotExist client s k attributes = do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
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
        return $ 
          case returnCode of 
            HyperclientSuccess -> Right ()
            _                  -> Left returnCode
  return (handle, continuation)

-- int64_t
-- hyperclient_del(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, enum hyperclient_returncode* status);
hyperclientDelete :: Hyperclient -> ByteString -> ByteString
                     -> AsyncResultHandle ()
hyperclientDelete client s k = do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
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
        return $ 
          case returnCode of 
            HyperclientSuccess -> Right ()
            _                  -> Left returnCode
  return (handle, continuation)

-- int64_t
-- hyperclient_cond_put(struct hyperclient* client, const char* space,
--                      const char* key, size_t key_sz,
--                      const struct hyperclient_attribute_check* checks, size_t checks_sz,
--                      const struct hyperclient_attribute* attrs, size_t attrs_sz,
--                      enum hyperclient_returncode* status);
hyperclientConditionalPut :: Hyperclient -> ByteString -> ByteString
                             -> [AttributeCheck] -> [Attribute]
                             -> AsyncResultHandle ()
hyperclientConditionalPut client s k checks attributes = do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
  space <- newCBString s
  (key,keySize) <- newCBStringLen k
  (attributePtr, attributeSize) <- newHyperDexAttributeArray attributes
  (checkPtr, checkSize) <- newHyperDexAttributeCheckArray checks
  handle <- {# call hyperclient_cond_put #} 
              client
              space key (fromIntegral keySize)
              checkPtr (fromIntegral checkSize)
              attributePtr (fromIntegral attributeSize)
              returnCodePtr
  traceIO $ "In hyperclientConditionalPut"
  traceIO $ "  handle: " ++ show handle
  let continuation = do
        returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
        free returnCodePtr
        free space
        free key
        haskellFreeAttributes attributePtr attributeSize
        haskellFreeAttributeChecks checkPtr checkSize
        traceIO $ "In hyperclientConditionalPut"
        traceIO $ "  returnCode: " ++ show returnCode
        return $ 
          case returnCode of 
            HyperclientSuccess -> Right ()
            _                  -> Left returnCode
  return (handle, continuation)