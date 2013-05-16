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
  , getAsync, putAsync
  , ReturnCode (..)
  , Attribute (..)
  , AsyncResult, Result
  )
  where

import Database.HyperDex.Internal.Client (Client, makeClient, closeClient, AsyncResult, Result)
import Database.HyperDex.Internal.Hyperdata (HyperSerialize)
import Database.HyperDex.Internal.ReturnCode (ReturnCode (..))
import Database.HyperDex.Internal.Attribute (Attribute (..))

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

getAsync :: HyperSerialize a => Text -> a -> AsyncResult [Attribute]
getAsync = undefined

putAsync :: a
putAsync = undefined
