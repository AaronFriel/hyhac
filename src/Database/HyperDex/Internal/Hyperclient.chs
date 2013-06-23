module Database.HyperDex.Internal.Hyperclient
  ( -- Simple, single-key operations 
    get
  , put
  , putIfNotExist
  , delete 
  , putConditional
    -- Atomic numeric operations
  , atomicAdd, atomicSub
  , atomicMul, atomicDiv
    -- Atomic integral operations
  , atomicMod
  , atomicAnd, atomicOr
  , atomicXor
    -- Atomic string operations
  , atomicStringPrepend, atomicStringAppend
  , atomicListLPush, atomicListRPush
  , atomicSetAdd, atomicSetRemove
  , atomicSetUnion, atomicSetIntersect
    -- Atomic simple map operations
  , atomicMapInsert, atomicMapDelete
  -- , hyperMapInsertConditional
    -- Atomic numeric value operations
  , atomicMapAdd, atomicMapSub
  , atomicMapMul, atomicMapDiv
    -- Atomic integral value operations
  , atomicMapMod
  , atomicMapAnd, atomicMapOr
  , atomicMapXor
    -- Atomic string value operations 
  , atomicMapStringPrepend 
  , atomicMapStringAppend
  -- Search operations
  , search
  , deleteGroup
  )
  where

import Foreign
import Foreign.C

import Data.ByteString (ByteString)
import Data.Text (Text)

#include "hyperclient.h"

{# import Database.HyperDex.Internal.ReturnCode #}
{# import Database.HyperDex.Internal.Client #}
{# import Database.HyperDex.Internal.Attribute #}
{# import Database.HyperDex.Internal.AttributeCheck #}
{# import Database.HyperDex.Internal.MapAttribute #}
import Database.HyperDex.Internal.Util

data Op = OpPut
        | OpPutIfNotExist
        | OpAtomicAdd
        | OpAtomicSub
        | OpAtomicMul
        | OpAtomicDiv
        | OpAtomicMod
        | OpAtomicAnd
        | OpAtomicOr
        | OpAtomicXor
        | OpAtomicStringPrepend
        | OpAtomicStringAppend
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

put :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
put = hyperclientOp OpPut

putIfNotExist :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putIfNotExist = hyperclientOp OpPutIfNotExist

atomicAdd, atomicSub,
  atomicMul, atomicDiv,
  atomicMod,
  atomicAnd, atomicOr,
  atomicXor,
  atomicStringPrepend, atomicStringAppend,
  atomicListLPush, atomicListRPush,
  atomicSetAdd, atomicSetRemove,
  atomicSetUnion, atomicSetIntersect :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()

atomicAdd          = hyperclientOp OpAtomicAdd 
atomicSub          = hyperclientOp OpAtomicSub 
atomicMul          = hyperclientOp OpAtomicMul 
atomicDiv          = hyperclientOp OpAtomicDiv 
atomicMod          = hyperclientOp OpAtomicMod 
atomicAnd          = hyperclientOp OpAtomicAnd 
atomicOr           = hyperclientOp OpAtomicOr 
atomicXor          = hyperclientOp OpAtomicXor 
atomicStringPrepend      = hyperclientOp OpAtomicStringPrepend 
atomicStringAppend       = hyperclientOp OpAtomicStringAppend 
atomicListLPush    = hyperclientOp OpAtomicListLPush 
atomicListRPush    = hyperclientOp OpAtomicListRPush 
atomicSetAdd       = hyperclientOp OpAtomicSetAdd 
atomicSetRemove    = hyperclientOp OpAtomicSetRemove 
atomicSetIntersect = hyperclientOp OpAtomicSetIntersect
atomicSetUnion     = hyperclientOp OpAtomicSetUnion 

atomicMapInsert,
  atomicMapDelete,
  atomicMapAdd,
  atomicMapSub,
  atomicMapMul,
  atomicMapDiv,
  atomicMapMod,
  atomicMapAnd,
  atomicMapOr,
  atomicMapXor,
  atomicMapStringPrepend, 
  atomicMapStringAppend :: Client -> Text -> ByteString -> [MapAttribute] -> AsyncResult () 

atomicMapInsert = hyperclientMapOp OpAtomicMapInsert
atomicMapDelete = hyperclientMapOp OpAtomicMapDelete
atomicMapAdd    = hyperclientMapOp OpAtomicMapAdd 
atomicMapSub    = hyperclientMapOp OpAtomicMapSub 
atomicMapMul    = hyperclientMapOp OpAtomicMapMul 
atomicMapDiv    = hyperclientMapOp OpAtomicMapDiv 
atomicMapMod    = hyperclientMapOp OpAtomicMapMod 
atomicMapAnd    = hyperclientMapOp OpAtomicMapAnd 
atomicMapOr     = hyperclientMapOp OpAtomicMapOr 
atomicMapXor    = hyperclientMapOp OpAtomicMapXor 
atomicMapStringPrepend = hyperclientMapOp OpAtomicMapStringPrepend
atomicMapStringAppend  = hyperclientMapOp OpAtomicMapStringAppend 

-- hyperMapInsertConditional :: Client -> Text -> ByteString -> [AttributeCheck] -> [MapAttribute] -> AsyncResult ()
-- hyperMapInsertConditional c s k checks attrs = withClient c $ \hc -> hyperclientConditionalMapInsert hc s k checks attrs

-- int64_t
-- hyperclient_put(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, const struct hyperclient_attribute* attrs,
--                 size_t attrs_sz, enum hyperclient_returncode* status);
get :: Client
    -> Text
    -> ByteString
    -> AsyncResult [Attribute] 
get client s k = withClient client $ \hyperclient -> do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
  attributePtrPtr <- malloc
  attributeSizePtr <- malloc
  space <- newTextUtf8 s
  (key,keySize) <- newCBStringLen k
  handle <- wrapHyperCall $
            {# call hyperclient_get #}
              hyperclient
              space key (fromIntegral keySize)
              returnCodePtr attributePtrPtr attributeSizePtr
  let continuation = do
        returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
        attributes <-
          case returnCode of
            HyperclientSuccess -> do
              attributePtr <- peek attributePtrPtr
              attributeSize <- fmap fromIntegral $ peek attributeSizePtr
              attrs <- peekArray attributeSize attributePtr
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
-- hyperclient_del(struct hyperclient* client, const char* space, const char* key,
--                 size_t key_sz, enum hyperclient_returncode* status);
delete :: Client
       -> Text
       -> ByteString
       -> AsyncResult ()
delete client s k = withClient client $ \hyperclient -> do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
  space <- newTextUtf8 s
  (key,keySize) <- newCBStringLen k
  handle <- wrapHyperCall $
            {# call hyperclient_del #}
              hyperclient
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
putConditional :: Client 
               -> Text
               -> ByteString
               -> [AttributeCheck]
               -> [Attribute]
               -> AsyncResult ()
putConditional client s k checks attributes = withClient client $ \hyperclient -> do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
  space <- newTextUtf8 s
  (key,keySize) <- newCBStringLen k
  (attributePtr, attributeSize) <- newHyperDexAttributeArray attributes
  (checkPtr, checkSize) <- newHyperDexAttributeCheckArray checks
  handle <- wrapHyperCall $
            {# call hyperclient_cond_put #}
              hyperclient
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
hyperclientOp :: Op
              -> Client
              -> Text
              -> ByteString
              -> [Attribute]
              -> AsyncResult ()
hyperclientOp op = 
  \client s k attributes ->
    withClient client $ \hyperclient -> do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
  space <- newTextUtf8 s
  (key,keySize) <- newCBStringLen k
  (attributePtr, attributeSize) <- newHyperDexAttributeArray attributes
  let ccall = case op of
              OpPut           -> {# call hyperclient_put #}
              OpPutIfNotExist -> {# call hyperclient_put_if_not_exist #}
              OpAtomicAdd     -> {# call hyperclient_atomic_add #}
              OpAtomicSub     -> {# call hyperclient_atomic_sub #}
              OpAtomicMul     -> {# call hyperclient_atomic_mul #}
              OpAtomicDiv     -> {# call hyperclient_atomic_div #}
              OpAtomicMod     -> {# call hyperclient_atomic_mod #}
              OpAtomicAnd     -> {# call hyperclient_atomic_and #}
              OpAtomicOr      -> {# call hyperclient_atomic_or  #}
              OpAtomicXor     -> {# call hyperclient_atomic_xor #}
              OpAtomicStringPrepend -> {# call hyperclient_string_prepend #}
              OpAtomicStringAppend  -> {# call hyperclient_string_append  #}
              OpAtomicListLPush     -> {# call hyperclient_list_lpush #}
              OpAtomicListRPush     -> {# call hyperclient_list_rpush #}
              OpAtomicSetAdd        -> {# call hyperclient_set_add       #}
              OpAtomicSetRemove     -> {# call hyperclient_set_remove    #}
              OpAtomicSetIntersect  -> {# call hyperclient_set_intersect #}
              OpAtomicSetUnion      -> {# call hyperclient_set_union     #}
  handle <- wrapHyperCall $
            ccall
              hyperclient
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
{-# INLINE hyperclientOp #-}

-- int64_t
-- hyperclient_map_add(struct hyperclient* client, const char* space,
--                     const char* key, size_t key_sz,
--                     const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
--                     enum hyperclient_returncode* status);
hyperclientMapOp :: MapOp 
                 -> Client
                 -> Text
                 -> ByteString
                 -> [MapAttribute]
                 -> AsyncResult ()
hyperclientMapOp op =
  \client s k mapAttributes ->
    withClient client $ \hyperclient -> do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
  space <- newTextUtf8 s
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
  handle <- wrapHyperCall $
            ccall
              hyperclient space
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
{-# INLINE hyperclientMapOp #-}

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

--hyperclientConditionalMapInsert :: Hyperclient -> Text -> ByteString
--                                -> [AttributeCheck]
--                                -> [MapAttribute]
--                                -> AsyncResultHandle ()
--hyperclientConditionalMapInsert client s k checks mapAttributes = do
--  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
--  space <- newTextUtf8 s
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
          -> Text
          -> [AttributeCheck] 
          -> AsyncResult (SearchStream [Attribute])
search client s checks = withClientStream client $ \hyperclient -> do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
  space <- newTextUtf8 s
  (checkPtr, checkSize) <- newHyperDexAttributeCheckArray checks
  resultSetPtrPtr <- malloc
  resultSetSizePtr <- malloc
  handle <- wrapHyperCall $
            {# call hyperclient_search #}
              hyperclient space
              checkPtr (fromIntegral checkSize :: {# type size_t #})
              returnCodePtr
              resultSetPtrPtr resultSetSizePtr
  case handle >= 0 of
    True -> do
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
                              Nothing -> HyperclientDupeattr
                              Just rc -> rc
      return (handle, continuation)
    False -> do
      let continuation = const $ do
            returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
            free returnCodePtr
            free space
            haskellFreeAttributeChecks checkPtr checkSize
            free resultSetPtrPtr
            free resultSetSizePtr
            return $ Left returnCode
      return (handle, continuation)

-- int64_t
-- hyperclient_group_del(struct hyperclient* client, const char* space,
--                       const struct hyperclient_attribute_check* checks, size_t checks_sz,
--                       enum hyperclient_returncode* status);
--
deleteGroup :: Client 
            -> Text
            -> [AttributeCheck]
            -> AsyncResult ()
deleteGroup client s checks = withClient client $ \hyperclient -> do
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperclientGarbage)
  space <- newTextUtf8 s
  (checkPtr, checkSize) <- newHyperDexAttributeCheckArray checks
  handle <- wrapHyperCall $
            {# call hyperclient_group_del #}
              hyperclient space
              checkPtr (fromIntegral checkSize)
              returnCodePtr
  let continuation = do
        returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
        free returnCodePtr
        free space
        haskellFreeAttributeChecks checkPtr checkSize
        return $ 
          case returnCode of 
            HyperclientSuccess -> Right ()
            _                  -> Left returnCode
  return (handle, continuation)
