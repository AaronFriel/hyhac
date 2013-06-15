{-# LANGUAGE ViewPatterns #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Data.HyperDex.Client
-- Copyright   :  (c) Aaron Friel 2013
-- License     :  BSD-style
-- Maintainer  :  mayreply@aaronfriel.com
-- Stability   :  maybe
-- Portability :  portable
--
-- A client connection based API for the HyperDex key value store.
--
-- This module exposes functions that rely on manually providing a client 
-- parameter, and no guarantees are provided that resources will be cleaned
-- up unless the end-user ensures the connection is closed.
--
-----------------------------------------------------------------------------

module Database.HyperDex.Client 
  ( Client
  , connect, close
  , ConnectInfo
  , defaultConnectInfo
  , ConnectOptions
  , defaultConnectOptions
  , addSpace, removeSpace
  , getAsyncAttr, putAsyncAttr
  , putIfNotExistAsync
  , deleteAsync
  , putConditionalAsyncAttr
  , putAtomicAdd, putAtomicSub
  , putAtomicMul, putAtomicDiv
  , putAtomicMod
  , putAtomicAnd, putAtomicOr
  , putAtomicXor
  , putAtomicPrepend, putAtomicAppend
  , putAtomicListLPush, putAtomicListRPush
  , putAtomicSetAdd, putAtomicSetRemove
  , putAtomicSetIntersect, putAtomicSetUnion
  -- Atomic map operations
  , putAtomicMapInsert, putAtomicMapDelete
  , putAtomicMapAdd, putAtomicMapSub
  , putAtomicMapMul, putAtomicMapDiv
  , putAtomicMapMod
  , putAtomicMapAnd, putAtomicMapOr
  , putAtomicMapXor
  , putAtomicMapStringPrepend 
  , putAtomicMapStringAppend
  , ReturnCode (..)
  , Attribute (..), mkAttribute
  , AttributeCheck (..), mkAttributeCheck
  , MapAttribute (..), mkMapAttribute, mkMapAttributesFromMap
  , AsyncResult, Result
  , Hyperdatatype (..)
  , HyperSerialize
  , serialize, deserialize, datatype
  , Hyperpredicate (..)
  )
  where

import Database.HyperDex.Internal.Client ( 
        Client, connect, close, AsyncResult, Result
      , ConnectInfo, defaultConnectInfo
      , ConnectOptions, defaultConnectOptions
      )
import Database.HyperDex.Internal.Hyperclient
import Database.HyperDex.Internal.Hyperdex (Hyperdatatype (..), Hyperpredicate (..))
import Database.HyperDex.Internal.Hyperdata (HyperSerialize, serialize, deserialize, datatype)
import Database.HyperDex.Internal.ReturnCode (ReturnCode (..))
import Database.HyperDex.Internal.Attribute (Attribute (..), mkAttribute)
import Database.HyperDex.Internal.AttributeCheck (AttributeCheck (..), mkAttributeCheck)
import Database.HyperDex.Internal.MapAttribute (MapAttribute (..), mkMapAttribute, mkMapAttributesFromMap)
import qualified Database.HyperDex.Internal.Space as Space (addSpace, removeSpace)

import Data.ByteString (ByteString)

import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)

-- | Create a space from a definition.
addSpace :: Client -> Text -> IO ReturnCode
addSpace client (encodeUtf8 -> spaceDef) = Space.addSpace client spaceDef

-- | Remove a space by name.
removeSpace :: Client -> Text -> IO ReturnCode
removeSpace client (encodeUtf8 -> space) = Space.removeSpace client space

-- | Retrieve a value from a space by key.
getAsyncAttr :: Client -> Text -> ByteString -> AsyncResult [Attribute]
getAsyncAttr client (encodeUtf8 -> space) key =
  hyperGet client space key

-- | Put a value in a space by key.
putAsyncAttr :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAsyncAttr client (encodeUtf8 -> space) key attrs = 
  hyperPut client space key attrs

putIfNotExistAsync :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putIfNotExistAsync client (encodeUtf8 -> space) key attrs = 
  hyperPutIfNotExist client space key attrs

deleteAsync :: Client -> Text -> ByteString -> AsyncResult ()
deleteAsync client (encodeUtf8 -> space) key = 
  hyperDelete client space key

putConditionalAsyncAttr :: Client -> Text -> ByteString -> [AttributeCheck] -> [Attribute] -> AsyncResult ()
putConditionalAsyncAttr client (encodeUtf8 -> space) key checks attrs = 
  hyperPutConditionally client space key checks attrs

putAtomicAdd :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicAdd client (encodeUtf8 -> space) key attrs = 
  hyperAtomicAdd client space key attrs

putAtomicSub :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicSub client (encodeUtf8 -> space) key attrs = 
  hyperAtomicSub client space key attrs

putAtomicMul :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicMul client (encodeUtf8 -> space) key attrs =
  hyperAtomicMul client space key attrs

putAtomicDiv :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicDiv client (encodeUtf8 -> space) key attrs = 
  hyperAtomicDiv client space key attrs

putAtomicMod :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicMod client (encodeUtf8 -> space) key attrs = 
  hyperAtomicMod client space key attrs

putAtomicAnd :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicAnd client (encodeUtf8 -> space) key attrs = 
  hyperAtomicAnd client space key attrs

putAtomicOr :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicOr client (encodeUtf8 -> space) key attrs = 
  hyperAtomicOr client space key attrs

putAtomicXor :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicXor client (encodeUtf8 -> space) key attrs = 
  hyperAtomicXor client space key attrs

putAtomicPrepend :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicPrepend client (encodeUtf8 -> space) key attrs = 
  hyperAtomicPrepend client space key attrs

putAtomicAppend :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicAppend client (encodeUtf8 -> space) key attrs = 
  hyperAtomicAppend client space key attrs

putAtomicListLPush :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicListLPush client (encodeUtf8 -> space) key attrs = 
  hyperAtomicListLPush client space key attrs

putAtomicListRPush :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicListRPush client (encodeUtf8 -> space) key attrs = 
  hyperAtomicListRPush client space key attrs

putAtomicSetAdd :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicSetAdd client (encodeUtf8 -> space) key attrs = 
  hyperAtomicSetAdd client space key attrs

putAtomicSetRemove :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicSetRemove client (encodeUtf8 -> space) key attrs = 
  hyperAtomicSetRemove client space key attrs

putAtomicSetIntersect :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicSetIntersect client (encodeUtf8 -> space) key attrs = 
  hyperAtomicSetIntersect client space key attrs

putAtomicSetUnion :: Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()
putAtomicSetUnion client (encodeUtf8 -> space) key attrs = 
  hyperAtomicSetUnion client space key attrs

putAtomicMapInsert,
  putAtomicMapDelete,
  putAtomicMapAdd,
  putAtomicMapSub,
  putAtomicMapMul,
  putAtomicMapDiv,
  putAtomicMapMod,
  putAtomicMapAnd,
  putAtomicMapOr,
  putAtomicMapXor,
  putAtomicMapStringPrepend, 
  putAtomicMapStringAppend :: Client -> Text -> ByteString -> [MapAttribute] -> AsyncResult ()
putAtomicMapInsert client = hyperAtomicMapInsert client . encodeUtf8
putAtomicMapDelete client = hyperAtomicMapDelete client . encodeUtf8
putAtomicMapAdd    client = hyperAtomicMapAdd    client . encodeUtf8
putAtomicMapSub    client = hyperAtomicMapSub    client . encodeUtf8
putAtomicMapMul    client = hyperAtomicMapMul    client . encodeUtf8
putAtomicMapDiv    client = hyperAtomicMapDiv    client . encodeUtf8
putAtomicMapMod    client = hyperAtomicMapMod    client . encodeUtf8
putAtomicMapAnd    client = hyperAtomicMapAnd    client . encodeUtf8
putAtomicMapOr     client = hyperAtomicMapOr     client . encodeUtf8
putAtomicMapXor    client = hyperAtomicMapXor    client . encodeUtf8
putAtomicMapStringPrepend client = hyperAtomicMapStringPrepend client . encodeUtf8 
putAtomicMapStringAppend  client = hyperAtomicMapStringAppend  client . encodeUtf8 