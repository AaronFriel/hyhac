
-- |
-- Module       : Database.HyperDex.Internal.HyperdexClient
-- Copyright    : (c) Aaron Friel 2013-2014
--                (c) Niklas Hambüchen 2013-2014 
-- License      : BSD-style
-- Maintainer   : mayreply@aaronfriel.com
-- Stability    : unstable
-- Portability  : portable
--
module Database.HyperDex.Internal.HyperdexClient
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
  , stringPrepend, stringAppend
  , listLPush, listRPush
  , setAdd, setRemove
  , setUnion, setIntersect
    -- Atomic simple map operations
  , mapAdd, mapRemove
  -- , atomicConditionalMapInsert
    -- Atomic numeric value operations
  , mapAtomicAdd, mapAtomicSub
  , mapAtomicMul, mapAtomicDiv
    -- Atomic integral value operations
  , mapAtomicMod
  , mapAtomicAnd, mapAtomicOr
  , mapAtomicXor
    -- Atomic string value operations
  , mapStringPrepend
  , mapStringAppend
  -- Search operations
  , search
  , describeSearch
  , deleteGroup
  , count
  )
  where

import Foreign
import Foreign.C

import Data.ByteString (ByteString, packCString)
import Data.Word

import Control.Monad.IO.Class

#include "hyperdex/client.h"

{# import Database.HyperDex.Internal.Client #}
{# import Database.HyperDex.Internal.Attribute #}
{# import Database.HyperDex.Internal.AttributeCheck #}
{# import Database.HyperDex.Internal.MapAttribute #}
import Database.HyperDex.Internal.Core
import Database.HyperDex.Internal.Handle (wrapHyperCallHandle)
import Database.HyperDex.Internal.Util

type ClientCall = Client
                -> CString 
                -> CString -> CULong
                -> Ptr Attribute -> CULong
                -> Ptr CInt
                -> IO CLong

type MapCall = Client
             -> CString 
             -> CString -> CULong
             -> Ptr MapAttribute -> CULong
             -> Ptr CInt
             -> IO CLong

put           = hyperdexClientOp {# call unsafe hyperdex_client_put              #}
putIfNotExist = hyperdexClientOp {# call unsafe hyperdex_client_put_if_not_exist #}
atomicAdd     = hyperdexClientOp {# call unsafe hyperdex_client_atomic_add #}
atomicSub     = hyperdexClientOp {# call unsafe hyperdex_client_atomic_sub #}
atomicMul     = hyperdexClientOp {# call unsafe hyperdex_client_atomic_mul #}
atomicDiv     = hyperdexClientOp {# call unsafe hyperdex_client_atomic_div #}
atomicMod     = hyperdexClientOp {# call unsafe hyperdex_client_atomic_mod #}
atomicAnd     = hyperdexClientOp {# call unsafe hyperdex_client_atomic_and #}
atomicOr      = hyperdexClientOp {# call unsafe hyperdex_client_atomic_or  #}
atomicXor     = hyperdexClientOp {# call unsafe hyperdex_client_atomic_xor #}
stringPrepend = hyperdexClientOp {# call unsafe hyperdex_client_string_prepend #}
stringAppend  = hyperdexClientOp {# call unsafe hyperdex_client_string_append  #}
listLPush     = hyperdexClientOp {# call unsafe hyperdex_client_list_lpush    #}
listRPush     = hyperdexClientOp {# call unsafe hyperdex_client_list_rpush    #}
setAdd        = hyperdexClientOp {# call unsafe hyperdex_client_set_add       #}
setRemove     = hyperdexClientOp {# call unsafe hyperdex_client_set_remove    #}
setIntersect  = hyperdexClientOp {# call unsafe hyperdex_client_set_intersect #}
setUnion      = hyperdexClientOp {# call unsafe hyperdex_client_set_union     #}

-- mapAtomicInsert        = hyperdexClientMapOp {# call unsafe hyperdex_client_map_atomic_insert #}
mapAdd                 = hyperdexClientMapOp {# call unsafe hyperdex_client_map_add #}
mapRemove              = hyperdexClientOp {# call unsafe hyperdex_client_map_remove #}
mapAtomicAdd           = hyperdexClientMapOp {# call unsafe hyperdex_client_map_atomic_add #}
mapAtomicSub           = hyperdexClientMapOp {# call unsafe hyperdex_client_map_atomic_sub #}
mapAtomicMul           = hyperdexClientMapOp {# call unsafe hyperdex_client_map_atomic_mul #}
mapAtomicDiv           = hyperdexClientMapOp {# call unsafe hyperdex_client_map_atomic_div #}
mapAtomicMod           = hyperdexClientMapOp {# call unsafe hyperdex_client_map_atomic_mod #}
mapAtomicAnd           = hyperdexClientMapOp {# call unsafe hyperdex_client_map_atomic_and #}
mapAtomicOr            = hyperdexClientMapOp {# call unsafe hyperdex_client_map_atomic_or  #}
mapAtomicXor           = hyperdexClientMapOp {# call unsafe hyperdex_client_map_atomic_xor #}
mapStringPrepend = hyperdexClientMapOp {# call unsafe hyperdex_client_map_string_prepend #}
mapStringAppend  = hyperdexClientMapOp {# call unsafe hyperdex_client_map_string_append  #}

-- mapAtomicInsert = hyperdexClientMapOp OpAtomicMapInsert
-- mapAtomicAdd    = hyperdexClientMapOp OpAtomicMapAdd
-- mapAtomicSub    = hyperdexClientMapOp OpAtomicMapSub
-- mapAtomicMul    = hyperdexClientMapOp OpAtomicMapMul
-- mapAtomicDiv    = hyperdexClientMapOp OpAtomicMapDiv
-- mapAtomicMod    = hyperdexClientMapOp OpAtomicMapMod
-- mapAtomicAnd    = hyperdexClientMapOp OpAtomicMapAnd
-- mapAtomicOr     = hyperdexClientMapOp OpAtomicMapOr
-- mapAtomicXor    = hyperdexClientMapOp OpAtomicMapXor
-- mapAtomicStringPrepend = hyperdexClientMapOp OpAtomicMapStringPrepend
-- mapAtomicStringAppend  = hyperdexClientMapOp OpAtomicMapStringAppend

--hyperdexClientMapOp = undefined
--hyperdexClientOp = undefined
--putConditional = undefined
--search = undefined
--describeSearch = undefined
--deleteGroup = undefined
--count = undefined


-- int64_t
-- hyperdex_client_get(struct hyperdex_client* client,
--                     const char* space,
--                     const char* key, size_t key_sz,
--                     enum hyperdex_client_returncode* status,
--                     const struct hyperdex_client_attribute** attrs, size_t* attrs_sz);
get :: ByteString
    -> ByteString
    -> HyperDexConnection Client
    -> IO (AsyncResult Client [Attribute])
get s k = wrapDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ ClientGarbage)
  space <- rNewCBString0 s
  (key,keySize) <- rNewCBStringLen k
  (attrPtrPtr, attrSzPtr, peekResult) <- rMallocAttributeArray
  let ccall ptr = 
        wrapHyperCallHandle $
          {# call unsafe hyperdex_client_get #}
            ptr
            space key (fromIntegral keySize)
            returnCodePtr attrPtrPtr attrSzPtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          ClientSuccess -> fmap Right peekResult
          _             -> return $ Left returnCode
  return $ Call ccall callback

-- int64_t
-- hyperdex_client_del(struct hyperdex_client* client,
--                     const char* space,
--                     const char* key, size_t key_sz,
--                     enum hyperdex_client_returncode* status);
delete :: ByteString
       -> ByteString
       -> HyperDexConnection Client
       -> IO (AsyncResult Client ())
delete s k = wrapDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ ClientGarbage)
  space <- rNewCBString0 s
  (key,keySize) <- rNewCBStringLen k
  let ccall ptr = 
        wrapHyperCallHandle $
          {# call unsafe hyperdex_client_del #}
            ptr
            space key (fromIntegral keySize)
            returnCodePtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          ClientSuccess -> return $ Right ()
          _             -> return $ Left returnCode
  return $ Call ccall callback

-- int64_t
-- hyperdex_client_cond_put(struct hyperdexClient* client, const char* space,
--                          const char* key, size_t key_sz,
--                          const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
--                          const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
--                          enum hyperdex_client_returncode* status);
putConditional :: ByteString
               -> ByteString
               -> [AttributeCheck]
               -> [Attribute]
               -> HyperDexConnection Client
               -> IO (AsyncResult Client ())
putConditional s k checks attrs = wrapDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ ClientGarbage)
  space <- rNewCBString0 s
  (key,keySize) <- rNewCBStringLen k
  (attributePtr, attributeSize) <- rNewAttributeArray attrs
  (checkPtr, checkSize) <- rNewAttributeCheckArray checks
  let ccall ptr = 
        wrapHyperCallHandle $
          {# call unsafe hyperdex_client_cond_put #}
            ptr
            space key (fromIntegral keySize)
            checkPtr (fromIntegral checkSize)
            attributePtr (fromIntegral attributeSize)
            returnCodePtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          ClientSuccess -> return $ Right ()
          _             -> return $ Left returnCode
  return $ Call ccall callback

-- int64_t
-- hyperdex_client_atomic_xor(struct hyperdexClient* client, const char* space,
--                            const char* key, size_t key_sz,
--                            const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
--                            enum hyperdex_client_returncode* status);
hyperdexClientOp :: ClientCall
                 -> ByteString
                 -> ByteString
                 -> [Attribute]
                 -> HyperDexConnection Client
                 -> IO (AsyncResult Client ())
hyperdexClientOp call s k attrs = wrapDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ ClientGarbage)
  space <- rNewCBString0 s
  (key,keySize) <- rNewCBStringLen k
  (attributePtr, attributeSize) <- rNewAttributeArray attrs
  let ccall ptr = 
        wrapHyperCallHandle $
          call
            ptr
            space key (fromIntegral keySize)
            attributePtr (fromIntegral attributeSize)
            returnCodePtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          ClientSuccess -> return $ Right ()
          _             -> return $ Left returnCode
  return $ Call ccall callback
{-# INLINE hyperdexClientOp #-}

-- int64_t
-- hyperdex_client_map_add(struct hyperdexClient* client, const char* space,
--                         const char* key, size_t key_sz,
--                         const struct hyperdex_client_map_attribute* attrs, size_t attrs_sz,
--                         enum hyperdex_client_returncode* status);
hyperdexClientMapOp :: MapCall
                    -> ByteString
                    -> ByteString
                    -> [MapAttribute]
                    -> HyperDexConnection Client
                    -> IO (AsyncResult Client ())
hyperdexClientMapOp call s k mapAttrs = wrapDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ ClientGarbage)
  space <- rNewCBString0 s
  (key,keySize) <- rNewCBStringLen k
  (mapAttributePtr, mapAttributeSize) <- rNewMapAttributeArray mapAttrs
  let ccall ptr =
        wrapHyperCallHandle $
          call
            ptr space
            key (fromIntegral keySize)
            mapAttributePtr (fromIntegral mapAttributeSize)
            returnCodePtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          ClientSuccess -> return $ Right ()
          _             -> return $ Left returnCode
  return $ Call ccall callback
{-# INLINE hyperdexClientMapOp #-}

-- -- int64_t
-- -- hyperdex_client_search(struct hyperdexClient* client, const char* space,
-- --                        const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
-- --                        enum hyperdex_client_returncode* status,
-- --                        struct hyperdex_client_attribute** attrs, size_t* attrs_sz);
--search :: HyperDexConnection Client
--       -> ByteString
--       -> [AttributeCheck]
--       -> IO (SearchStream [Attribute])
search s checks = wrapIterator $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ ClientGarbage)
  space <- rNewCBString0 s
  (checkPtr, checkSize) <- rNewAttributeCheckArray checks
  (resultPtrPtr, resultSizePtr, peekResult) <- rMallocAttributeArray
  let ccall ptr =
        wrapHyperCallHandle $
          {# call unsafe hyperdex_client_search #}
            ptr
            space
            checkPtr (fromIntegral checkSize)
            returnCodePtr
            resultPtrPtr resultSizePtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          ClientSuccess -> fmap Right peekResult
          _             -> return $ Left returnCode
  return $ Call ccall callback


-- int64_t
-- hyperdex_client_group_del(struct hyperdex_client* client,
--                           const char* space,
--                           const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
--                           enum hyperdex_client_returncode* status);
deleteGroup :: ByteString
            -> [AttributeCheck]
            -> HyperDexConnection Client
            -> IO (AsyncResult Client ())
deleteGroup s checks = wrapDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ ClientGarbage)
  space <- rNewCBString0 s
  (checkPtr, checkSize) <- rNewAttributeCheckArray checks
  let ccall ptr =
        wrapHyperCallHandle $
          {# call unsafe hyperdex_client_group_del #}
            ptr
            space
            checkPtr (fromIntegral checkSize)
            returnCodePtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          ClientSuccess -> return $ Right ()
          _             -> return $ Left returnCode
  return $ Call ccall callback

-- int64_t
-- hyperdex_client_search_describe(struct hyperdexClient* client, const char* space,
--                                 const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
--                                 enum hyperdex_client_returncode* status, const char** description);
describeSearch :: ByteString
               -> [AttributeCheck]
               -> HyperDexConnection Client
               -> IO (AsyncResult Client ByteString)
describeSearch s checks = wrapDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ ClientGarbage)
  space <- rNewCBString0 s
  (checkPtr, checkSize) <- rNewAttributeCheckArray checks
  descriptionPtr <- rMalloc
  let ccall ptr =
        wrapHyperCallHandle $
          {# call unsafe hyperdex_client_search_describe #}
            ptr
            space
            checkPtr (fromIntegral checkSize)
            returnCodePtr
            descriptionPtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        -- TODO: Who is responsible for freeing the description string?
        case returnCode of
          ClientSuccess -> liftIO $ do
                              cStr <- peek descriptionPtr
                              description <- packCString cStr
                              return $ Right description
          _             -> return $ Left returnCode
  return $ Call ccall callback

unCULong :: CULong -> Word64
unCULong (CULong w) = w

-- int64_t
-- hyperdex_client_count(struct hyperdexClient* client, const char* space,
--                       const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
--                       enum hyperdex_client_returncode* status, uint64_t* result);
count :: ByteString
      -> [AttributeCheck]
      -> HyperDexConnection Client
      -> IO (AsyncResult Client Word64)
count s checks = wrapDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ ClientGarbage)
  space <- rNewCBString0 s
  (checkPtr, checkSize) <- rNewAttributeCheckArray checks
  countPtr <- rMalloc
  let ccall ptr =
        wrapHyperCallHandle $
          {# call unsafe hyperdex_client_count #}
            ptr
            space
            checkPtr (fromIntegral checkSize)
            returnCodePtr
            countPtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        -- TODO: Who is responsible for freeing the description string?
        case returnCode of
          ClientSuccess -> liftIO $ fmap (Right . unCULong) $ peek countPtr 
          _             -> return $ Left returnCode
  return $ Call ccall callback
