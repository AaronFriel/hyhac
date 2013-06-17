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
  , hyperAtomicSetAdd, hyperAtomicSetRemove
  , hyperAtomicSetUnion, hyperAtomicSetIntersect
  -- Atomic map operations
  , hyperAtomicMapInsert, hyperAtomicMapDelete
  , hyperAtomicMapAdd, hyperAtomicMapSub
  , hyperAtomicMapMul, hyperAtomicMapDiv
  , hyperAtomicMapMod
  , hyperAtomicMapAnd, hyperAtomicMapOr
  , hyperAtomicMapXor
  , hyperAtomicMapStringPrepend 
  , hyperAtomicMapStringAppend
  -- , hyperMapInsertConditional
  -- Search operations
  , search
  , SearchStream (..)
  )
  where

{# import Database.HyperDex.Internal.ReturnCode #}
{# import Database.HyperDex.Internal.Client #}
{# import Database.HyperDex.Internal.Attribute #}
{# import Database.HyperDex.Internal.AttributeCheck #}
{# import Database.HyperDex.Internal.MapAttribute #}
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
        | OpAtomicSetAdd
        | OpAtomicSetRemove
        | OpAtomicSetIntersect
        | OpAtomicSetUnion

data MapOp = OpAtomicMapInsert
           | OpAtomicMapDelete
           | OpAtomicMapAdd
           | OpAtomicMapSub
           | OpAtomicMapMul
           | OpAtomicMapDiv
           | OpAtomicMapMod
           | OpAtomicMapAnd
           | OpAtomicMapOr
           | OpAtomicMapXor
           | OpAtomicMapStringPrepend
           | OpAtomicMapStringAppend

hyperGet :: Client -> ByteString -> ByteString
            -> AsyncResult [Attribute]
hyperGet c s k = withClient c (\hc -> hyperclientGet hc s k)
{-# INLINE hyperGet #-}

hyperPut :: Client -> ByteString -> ByteString -> [Attribute]
            -> AsyncResult ()
hyperPut c s k attrs = withClient c (\hc -> hyperclientPut hc s k attrs)
{-# INLINE hyperPut #-}

hyperPutIfNotExist :: Client -> ByteString -> ByteString -> [Attribute]
                      -> AsyncResult ()
hyperPutIfNotExist c s k attrs = withClient c (\hc -> hyperclientPutIfNotExist hc s k attrs)
{-# INLINE hyperPutIfNotExist #-}

hyperDelete :: Client -> ByteString -> ByteString
               -> AsyncResult ()
hyperDelete c s k = withClient c (\hc -> hyperclientDelete hc s k)
{-# INLINE hyperDelete #-}

hyperPutConditionally :: Client -> ByteString -> ByteString
                         -> [AttributeCheck] -> [Attribute]
                         -> AsyncResult ()
hyperPutConditionally c s k checks attrs = withClient c (\hc -> hyperclientConditionalPut hc s k checks attrs)
{-# INLINE hyperPutConditionally #-}

hyperAtomic :: Op -> Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()
hyperAtomic op = \c s k attrs -> withClient c (\hc -> hyperclientAtomicOp op hc s k attrs)
{-# INLINE hyperAtomic #-}

hyperAtomicAdd, hyperAtomicSub,
  hyperAtomicMul, hyperAtomicDiv,
  hyperAtomicMod,
  hyperAtomicAnd, hyperAtomicOr,
  hyperAtomicXor,
  hyperAtomicPrepend, hyperAtomicAppend,
  hyperAtomicListLPush, hyperAtomicListRPush,
  hyperAtomicSetAdd, hyperAtomicSetRemove,
  hyperAtomicSetUnion, hyperAtomicSetIntersect :: Client -> ByteString -> ByteString -> [Attribute] -> AsyncResult ()

hyperAtomicAdd          = hyperAtomic OpAtomicAdd 
{-# INLINE hyperAtomicAdd #-}
hyperAtomicSub          = hyperAtomic OpAtomicSub 
{-# INLINE hyperAtomicSub #-}
hyperAtomicMul          = hyperAtomic OpAtomicMul 
{-# INLINE hyperAtomicMul #-}
hyperAtomicDiv          = hyperAtomic OpAtomicDiv 
{-# INLINE hyperAtomicDiv #-}
hyperAtomicMod          = hyperAtomic OpAtomicMod 
{-# INLINE hyperAtomicMod #-}
hyperAtomicAnd          = hyperAtomic OpAtomicAnd 
{-# INLINE hyperAtomicAnd #-}
hyperAtomicOr           = hyperAtomic OpAtomicOr 
{-# INLINE hyperAtomicOr  #-}
hyperAtomicXor          = hyperAtomic OpAtomicXor 
{-# INLINE hyperAtomicXor #-}
hyperAtomicPrepend      = hyperAtomic OpAtomicPrepend 
{-# INLINE hyperAtomicPrepend   #-}
hyperAtomicAppend       = hyperAtomic OpAtomicAppend 
{-# INLINE hyperAtomicAppend    #-}
hyperAtomicListLPush    = hyperAtomic OpAtomicListLPush 
{-# INLINE hyperAtomicListLPush #-}
hyperAtomicListRPush    = hyperAtomic OpAtomicListRPush 
{-# INLINE hyperAtomicListRPush #-}
hyperAtomicSetAdd       = hyperAtomic OpAtomicSetAdd 
{-# INLINE hyperAtomicSetAdd         #-}
hyperAtomicSetRemove    = hyperAtomic OpAtomicSetRemove 
{-# INLINE hyperAtomicSetRemove      #-}
hyperAtomicSetIntersect = hyperAtomic OpAtomicSetIntersect
{-# INLINE hyperAtomicSetIntersect   #-}
hyperAtomicSetUnion     = hyperAtomic OpAtomicSetUnion 
{-# INLINE hyperAtomicSetUnion       #-}

hyperAtomicMap :: MapOp -> Client -> ByteString -> ByteString -> [MapAttribute] -> AsyncResult ()
hyperAtomicMap op = \c s k attrs -> withClient c (\hc -> hyperclientAtomicMapOp op hc s k attrs)
{-# INLINE hyperAtomicMap #-}

hyperAtomicMapInsert,
  hyperAtomicMapDelete,
  hyperAtomicMapAdd,
  hyperAtomicMapSub,
  hyperAtomicMapMul,
  hyperAtomicMapDiv,
  hyperAtomicMapMod,
  hyperAtomicMapAnd,
  hyperAtomicMapOr,
  hyperAtomicMapXor,
  hyperAtomicMapStringPrepend, 
  hyperAtomicMapStringAppend :: Client -> ByteString -> ByteString -> [MapAttribute] -> AsyncResult () 
hyperAtomicMapInsert = hyperAtomicMap OpAtomicMapInsert
{-# INLINE hyperAtomicMapInsert #-}
hyperAtomicMapDelete = hyperAtomicMap OpAtomicMapDelete
{-# INLINE hyperAtomicMapDelete #-}
hyperAtomicMapAdd    = hyperAtomicMap OpAtomicMapAdd 
{-# INLINE hyperAtomicMapAdd #-}
hyperAtomicMapSub    = hyperAtomicMap OpAtomicMapSub 
{-# INLINE hyperAtomicMapSub #-}
hyperAtomicMapMul    = hyperAtomicMap OpAtomicMapMul 
{-# INLINE hyperAtomicMapMul #-}
hyperAtomicMapDiv    = hyperAtomicMap OpAtomicMapDiv 
{-# INLINE hyperAtomicMapDiv #-}
hyperAtomicMapMod    = hyperAtomicMap OpAtomicMapMod 
{-# INLINE hyperAtomicMapMod #-}
hyperAtomicMapAnd    = hyperAtomicMap OpAtomicMapAnd 
{-# INLINE hyperAtomicMapAnd #-}
hyperAtomicMapOr     = hyperAtomicMap OpAtomicMapOr 
{-# INLINE hyperAtomicMapOr  #-}
hyperAtomicMapXor    = hyperAtomicMap OpAtomicMapXor 
{-# INLINE hyperAtomicMapXor #-}
hyperAtomicMapStringPrepend = hyperAtomicMap OpAtomicMapStringPrepend
{-# INLINE hyperAtomicMapStringPrepend #-}
hyperAtomicMapStringAppend  = hyperAtomicMap OpAtomicMapStringAppend 
{-# INLINE hyperAtomicMapStringAppend #-}

-- hyperMapInsertConditional :: Client -> ByteString -> ByteString -> [AttributeCheck] -> [MapAttribute] -> AsyncResult ()
-- hyperMapInsertConditional c s k checks attrs = withClient c $ \hc -> hyperclientConditionalMapInsert hc s k checks attrs

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
{-# INLINE hyperclientGet #-}

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
{-# INLINE hyperclientPut #-}

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
{-# INLINE hyperclientPutIfNotExist #-}

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
{-# INLINE hyperclientDelete #-}

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
{-# INLINE hyperclientConditionalPut #-}

-- int64_t
-- hyperclient_atomic_xor(struct hyperclient* client, const char* space,
--                        const char* key, size_t key_sz,
--                        const struct hyperclient_attribute* attrs, size_t attrs_sz,
--                        enum hyperclient_returncode* status);
hyperclientAtomicOp :: Op -> Hyperclient -> ByteString -> ByteString
                    -> [Attribute]
                    -> AsyncResultHandle ()
hyperclientAtomicOp op = \client s k attributes -> do
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
              OpAtomicSetAdd       -> {# call hyperclient_set_add       #}
              OpAtomicSetRemove    -> {# call hyperclient_set_remove    #}
              OpAtomicSetIntersect -> {# call hyperclient_set_intersect #}
              OpAtomicSetUnion     -> {# call hyperclient_set_union     #}
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

-- int64_t
-- hyperclient_map_add(struct hyperclient* client, const char* space,
--                     const char* key, size_t key_sz,
--                     const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
--                     enum hyperclient_returncode* status);
hyperclientAtomicMapOp :: MapOp -> Hyperclient -> ByteString -> ByteString
                    -> [MapAttribute]
                    -> AsyncResultHandle ()
hyperclientAtomicMapOp op = \client s k mapAttributes -> do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
  space <- newCBString s
  (key,keySize) <- newCBStringLen k
  (mapAttributePtr, mapAttributeSize) <- newHyperDexMapAttributeArray mapAttributes
  let ccall = case op of
              OpAtomicMapInsert -> {# call hyperclient_map_add    #}
              OpAtomicMapDelete -> {# call hyperclient_map_remove #}
              OpAtomicMapAdd    -> {# call hyperclient_map_atomic_add #}
              OpAtomicMapSub    -> {# call hyperclient_map_atomic_sub #}
              OpAtomicMapMul    -> {# call hyperclient_map_atomic_mul #}
              OpAtomicMapDiv    -> {# call hyperclient_map_atomic_div #}
              OpAtomicMapMod    -> {# call hyperclient_map_atomic_mod #}
              OpAtomicMapAnd    -> {# call hyperclient_map_atomic_and #}
              OpAtomicMapOr     -> {# call hyperclient_map_atomic_or  #}
              OpAtomicMapXor    -> {# call hyperclient_map_atomic_xor #}
              OpAtomicMapStringPrepend -> {# call hyperclient_map_string_prepend #}
              OpAtomicMapStringAppend  -> {# call hyperclient_map_string_append  #}
  handle <- ccall
              client space
              key (fromIntegral keySize)
              mapAttributePtr (fromIntegral mapAttributeSize)
              returnCodePtr
  let continuation = do
        returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
        free returnCodePtr
        free space
        free key
        --haskellFreeMapAttributes mapAttributePtr mapAttributeSize
        return $ 
          case returnCode of 
            HyperclientSuccess -> Right ()
            _                  -> Left returnCode
  return (handle, continuation)
{-# INLINE hyperclientAtomicMapOp #-}

-- int64_t
-- hyperclient_map_add(struct hyperclient* client, const char* space,
--                     const char* key, size_t key_sz,
--                     const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
--                     enum hyperclient_returncode* status);
---- A compilation error prevents this from being implemented:
-- /home/cloudium/git/hyhac/dist/build/libHShyhac-0.2.0.0_p.a(Hyperclient.p_o): In function `ra9O_info':
-- /tmp/ghc20379_0/ghc20379_1.p_o:(.text+0x1d6): undefined reference to `hyperclient_cond_map_add'
-- /home/cloudium/git/hyhac/dist/build/libHShyhac-0.2.0.0_p.a(Hyperclient.p_o): In function `sdlx_info':
-- /tmp/ghc20379_0/ghc20379_1.p_o:(.text+0x3d08d): undefined reference to `hyperclient_cond_map_add'
-- /home/cloudium/git/hyhac/dist/build/libHShyhac-0.2.0.0_p.a(Hyperclient.p_o): In function `sdm2_info':
-- /tmp/ghc20379_0/ghc20379_1.p_o:(.text+0x3dc85): undefined reference to `hyperclient_cond_map_add'

--hyperclientConditionalMapInsert :: Hyperclient -> ByteString -> ByteString
--                                -> [AttributeCheck]
--                                -> [MapAttribute]
--                                -> AsyncResultHandle ()
--hyperclientConditionalMapInsert client s k checks mapAttributes = do
--  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
--  space <- newCBString s
--  (key,keySize) <- newCBStringLen k
--  (checkPtr, checkSize) <- newHyperDexAttributeCheckArray checks
--  (mapAttributePtr, mapAttributeSize) <- newHyperDexMapAttributeArray mapAttributes
--  handle <- {# call hyperclient_cond_map_add #}
--              client space
--              key (fromIntegral keySize)
--              checkPtr (fromIntegral checkSize)
--              mapAttributePtr (fromIntegral mapAttributeSize)
--              returnCodePtr
--  let continuation = do
--        returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
--        free returnCodePtr
--        free space
--        free key
--        --haskellFreeMapAttributes mapAttributePtr mapAttributeSize
--        return $ 
--          case returnCode of 
--            HyperclientSuccess -> Right ()
--            _                  -> Left returnCode
--  return (handle, continuation)
--{-# INLINE hyperclientConditionalMapInsert #-}

-- int64_t
-- hyperclient_search(struct hyperclient* client, const char* space,
--                    const struct hyperclient_attribute_check* checks, size_t checks_sz,
--                    enum hyperclient_returncode* status,
--                    struct hyperclient_attribute** attrs, size_t* attrs_sz);
search :: Client
          -> ByteString
          -> [AttributeCheck] 
          -> AsyncResult (SearchStream [Attribute])
search client s checks = withClientStream client $ \hyperclient -> do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
  space <- newCBString s
  (checkPtr, checkSize) <- newHyperDexAttributeCheckArray checks
  resultSetPtrPtr <- malloc
  resultSetSizePtr <- malloc 
  handle <- {# call hyperclient_search #}
              hyperclient space
              checkPtr (fromIntegral checkSize :: {# type size_t #})
              returnCodePtr
              resultSetPtrPtr resultSetSizePtr
  let continuation (Just HyperclientSuccess) = do
        returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
        case returnCode of
          HyperclientSuccess -> do
            resultSize <- peek resultSetSizePtr
            resultSetPtr <- peek resultSetPtrPtr
            resultSet <- peekArray (fromIntegral resultSize) resultSetPtr
            return $ Right resultSet
          _ -> return $ Left returnCode
      continuation e = do
        free returnCodePtr
        free space
        haskellFreeAttributeChecks checkPtr checkSize
        free resultSetPtrPtr
        free resultSetSizePtr
        return $ Left $ case e of 
                          Nothing -> HyperclientGarbage
                          Just rc -> rc
  return (handle, continuation)
