module Database.HyperDex.Internal.Hyperclient
  ( hyperGet
  , hyperPut
  , hyperPutIfNotExist
  , hyperDelete 
  , hyperPutConditionally
  , hyperAtomicAdd, hyperAtomicSub
  , hyperAtomicMul, hyperAtomicDiv
  , hyperAtomicMod
  , hyperAtomicAnd, hyperAtomicOr
  , hyperAtomicXor
  , hyperAtomicPrepend, hyperAtomicAppend
  , hyperAtomicListLPush, hyperAtomicListRPush
  )
  where

{# import Database.HyperDex.Internal.ReturnCode #}
{# import Database.HyperDex.Internal.Client #}
{# import Database.HyperDex.Internal.Attribute #}
{# import Database.HyperDex.Internal.AttributeCheck #}
import Database.HyperDex.Internal.Util

#include "hyperclient.h"

data Op = OpAtomicAdd
        | OpAtomicSub
        | OpAtomicMul
        | OpAtomicDiv
        | OpAtomicMod
        | OpAtomicAnd
        | OpAtomicOr
        | OpAtomicXor
        | OpAtomicPrepend
        | OpAtomicAppend
        | OpAtomicListLPush
        | OpAtomicListRPush

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

hyperAtomic :: Op -> Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()
hyperAtomic op = \c s k attrs -> withClient c (\hc -> hyperclientAtomicOp op hc s k attrs)
{-# INLINE hyperAtomic #-}

hyperAtomicAdd :: Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()
hyperAtomicAdd = hyperAtomic OpAtomicAdd

hyperAtomicSub :: Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()
hyperAtomicSub = hyperAtomic OpAtomicSub

hyperAtomicMul :: Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()
hyperAtomicMul = hyperAtomic OpAtomicMul

hyperAtomicDiv :: Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()
hyperAtomicDiv = hyperAtomic OpAtomicDiv

hyperAtomicMod :: Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()
hyperAtomicMod = hyperAtomic OpAtomicMod

hyperAtomicAnd :: Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()
hyperAtomicAnd = hyperAtomic OpAtomicAnd

hyperAtomicOr  :: Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()
hyperAtomicOr  = hyperAtomic OpAtomicOr

hyperAtomicXor :: Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()
hyperAtomicXor = hyperAtomic OpAtomicXor

hyperAtomicPrepend :: Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()
hyperAtomicPrepend = hyperAtomic OpAtomicPrepend

hyperAtomicAppend :: Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()
hyperAtomicAppend = hyperAtomic OpAtomicAppend

hyperAtomicListLPush :: Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()
hyperAtomicListLPush = hyperAtomic OpAtomicListLPush

hyperAtomicListRPush :: Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()
hyperAtomicListRPush = hyperAtomic OpAtomicListRPush

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
  let continuation = do
        returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
        free returnCodePtr
        free space
        free key
        haskellFreeAttributes attributePtr attributeSize
        haskellFreeAttributeChecks checkPtr checkSize
        return $ 
          case returnCode of 
            HyperclientSuccess -> Right ()
            _                  -> Left returnCode
  return (handle, continuation)

-- int64_t
-- hyperclient_atomic_xor(struct hyperclient* client, const char* space,
--                        const char* key, size_t key_sz,
--                        const struct hyperclient_attribute* attrs, size_t attrs_sz,
--                        enum hyperclient_returncode* status);
hyperclientAtomicOp :: Op -> Hyperclient -> ByteString -> ByteString
                    -> [Attribute]
                    -> AsyncResultHandle ()
hyperclientAtomicOp op client s k attributes = do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
  space <- newCBString s
  (key,keySize) <- newCBStringLen k
  (attributePtr, attributeSize) <- newHyperDexAttributeArray attributes
  let ccall = case op of
              OpAtomicAdd -> {# call hyperclient_atomic_add #}
              OpAtomicSub -> {# call hyperclient_atomic_sub #}
              OpAtomicMul -> {# call hyperclient_atomic_mul #}
              OpAtomicDiv -> {# call hyperclient_atomic_div #}
              OpAtomicMod -> {# call hyperclient_atomic_mod #}
              OpAtomicAnd -> {# call hyperclient_atomic_and #}
              OpAtomicOr  -> {# call hyperclient_atomic_or  #}
              OpAtomicXor -> {# call hyperclient_atomic_xor #}
              OpAtomicPrepend -> {# call hyperclient_string_prepend #}
              OpAtomicAppend  -> {# call hyperclient_string_append  #}
              OpAtomicListLPush -> {# call hyperclient_list_lpush #}
              OpAtomicListRPush -> {# call hyperclient_list_rpush #}
  handle <- ccall
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
{-# INLINE hyperclientAtomicOp #-}
