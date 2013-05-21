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
  , connect', close
  , addSpace, removeSpace
  , getAsyncAttr, putAsyncAttr
  , ReturnCode (..)
  , Attribute (..)
  , mkAttribute
  , AsyncResult, Result
  , HyperSerialize
  , serialize, deserialize
  )
  where

import Database.HyperDex.Internal.Client 
  ( Client
  , makeClient, closeClient
  , AsyncResult, Result
  )
import Database.HyperDex.Internal.Hyperclient
  ( hyperGet, hyperPut )
import Database.HyperDex.Internal.Hyperdata (HyperSerialize, serialize, deserialize)
import Database.HyperDex.Internal.ReturnCode (ReturnCode (..))
import Database.HyperDex.Internal.Attribute (Attribute (..), mkAttribute)
import qualified Database.HyperDex.Internal.Space as Space (addSpace, removeSpace)

import Data.ByteString (ByteString)

import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)

-- | /Deprecated:/ Create a client connection.
--
-- This currently this will always return a connection but no guarantees
-- are made that it is open. This API is temporary.
connect' :: Text -> Int -> IO Client
connect' (encodeUtf8 -> host) (fromIntegral -> port) = do
  makeClient host port

-- | Close a client connection.
--
-- This additionally ensures all allocated resources are released on close.
close :: Client -> IO ()
close = closeClient

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
