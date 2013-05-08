{-# LANGUAGE ViewPatterns #-}

module Database.HyperDex.Internal.Client 
  ( Hyperclient, Client
  , Handle, Result
  , makeClient, closeClient
  , loopClient, loopClientUntil
  , withClient, withClientImmediate
  )
  where

{# import Database.HyperDex.Internal.ReturnCode #}
import Database.HyperDex.Internal.Util

import Data.Map (Map)
import qualified Data.Map as Map

import Control.Concurrent.MVar

#include "hyperclient.h"

{#pointer *hyperclient as Hyperclient #}

type ClientData =  MVar (Maybe Hyperclient, Map Handle (IO ()))

newtype Client = Client { unClient :: ClientData } 

type Handle = {# type int64_t #}

-- | A return value that contains a handle to use with hyperclient_loop and an IO action
-- to be performed to retrieve the result when the handle comes up from the loop.
type Result a = IO (Handle, IO a)

makeClient :: ByteString -> Int16 -> IO Client
makeClient host port = do
  client <- hyperclientCreate host port
  value <- newMVar (Just client, Map.empty)
  return $ Client value

closeClient :: Client -> IO ()
closeClient (unClient -> c) = do
  value <- takeMVar c
  case value of
    (Nothing, _)        -> error "HyperDex client error - cannot close a client connection twice."
    (Just hc, _ )  -> do
      hyperclientDestroy hc
      putMVar c (Nothing, Map.empty)

-- | Runs hyperclient_loop exactly once, setting the appropriate MVar.
loopClient :: Client -> IO (Maybe Handle)
loopClient (unClient -> c) = do
  value <- takeMVar c
  case value of
    (Nothing, _)        -> return Nothing
    (Just hc, handles)  -> do
      -- TODO: Handle errant condition (returnCode indicates client died)
      (handle, returnCode) <- hyperclientLoop hc 0
      case Map.lookup handle handles of
        Nothing -> do
          putMVar c (Just hc, handles)
          return Nothing
        Just f  -> do
          f
          putMVar c (Just hc, Map.delete handle handles)
          return $ Just handle

-- | Run hyperclient_loop at most N times or forever until a handle is returned.
loopClientUntil :: Client -> Handle -> Maybe Int -> MVar a -> IO (Bool)
loopClientUntil _      _ (Just 0) _ = return False

loopClientUntil client h (Just n) v = do
  empty <- isEmptyMVar v
  case empty of
    True -> do
      loopResult <- loopClient client
      case loopResult of
        Just handle -> if h == handle
                       then return True
                       else loopClientUntil client h (Just $ n - 1) v 
        _           -> loopClientUntil client h (Just $ n - 1) v
    False -> return True

loopClientUntil client h (Nothing) v = do
  empty <- isEmptyMVar v
  case empty of
    True -> do
      loopResult <- loopClient client
      case loopResult of
        Just handle -> if h == handle
                       then return True
                       else loopClientUntil client h (Nothing) v 
        _           -> loopClientUntil client h (Nothing) v
    False -> return True

promise :: Client -> Handle -> MVar a -> IO a
promise client h v = do
  loopResult <- loopClientUntil client h Nothing v 
  case loopResult of
    False -> error "This should not be possible (yet)"
    True  -> readMVar v

withClientImmediate :: Client -> (Hyperclient -> IO a) -> IO a
withClientImmediate (unClient -> c) f =
  withMVar c $ \value -> do
    case value of
      (Nothing, _) -> error "HyperDex client error - cannot use a closed connection."
      (Just hc, _) -> f hc

withClient :: Client -> (Hyperclient -> Result a) -> IO (IO a)
withClient client@(unClient -> c) f = do
  value <- takeMVar c
  case value of
    (Nothing, _)        -> error "HyperDex client error - cannot use a closed connection."
    (Just hc, handles)  -> do
      (h, cont) <- f hc
      v <- newEmptyMVar :: IO (MVar a)
      putMVar c (Just hc, Map.insert h (cont >>= putMVar v) handles)
      return $ promise client h v

-- struct hyperclient*
-- hyperclient_create(const char* coordinator, uint16_t port);
hyperclientCreate :: ByteString -> Int16 -> IO Hyperclient
hyperclientCreate h port = withCBString h $ \host -> {# call hyperclient_create #} host (fromIntegral port)

-- void
-- hyperclient_destroy(struct hyperclient* client);
hyperclientDestroy :: Hyperclient -> IO ()
hyperclientDestroy client = do
  {# call hyperclient_destroy #} client

-- int64_t
-- hyperclient_loop(struct hyperclient* client, int timeout,
--                  enum hyperclient_returncode* status);
hyperclientLoop :: Hyperclient -> Int -> IO (Handle, HyperclientReturnCode)
hyperclientLoop client timeout =
  alloca $ \returnCodePtr -> do
    handle <- {# call hyperclient_loop #} client (fromIntegral timeout) returnCodePtr
    returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
    return (handle, returnCode)
