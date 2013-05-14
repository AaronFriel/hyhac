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

import Data.Maybe

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
type Result a = IO (Handle, IO (Either HyperclientReturnCode a))

makeClient :: ByteString -> Int16 -> IO Client
makeClient host port = do
  client <- hyperclientCreate host port
  clientData <- newMVar (Just client, Map.empty)
  return $ Client clientData

closeClient :: Client -> IO ()
closeClient (unClient -> c) = do
  clientData <- takeMVar c
  case clientData of
    (Nothing, _)        -> error "HyperDex client error - cannot close a client connection twice."
    (Just hc, handles)  -> do
      hyperclientDestroy hc
      sequence_ $ Map.elems handles 
      putMVar c (Nothing, Map.empty)

-- | Runs hyperclient_loop exactly once, setting the appropriate MVar.
loopClient :: Client -> IO (Maybe Handle)
loopClient (unClient -> c) = do
  clientData <- takeMVar c
  case clientData of
    (Nothing, _)        -> return Nothing
    (Just hc, handles)  -> do
      (handle, returnCode) <- hyperclientLoop hc 0
      let f = fromMaybe (return ()) $ Map.lookup handle handles
      f
      putMVar c (Just hc, Map.delete handle handles)
      return $ Just handle

-- | Run hyperclient_loop at most N times or forever until a handle is returned.
loopClientUntil :: Client -> Handle -> Maybe Int -> MVar (Either HyperclientReturnCode a) -> IO (Bool)
loopClientUntil _      _ (Just 0) _ = return False

loopClientUntil client h (Just n) v = do
  empty <- isEmptyMVar v
  case empty of
    True -> do
      loopResult <- loopClient client
      clientData <- readMVar $ unClient client
      --  TODO: Exponential backoff or some other approach for polling
      case clientData of
        (Nothing, _)       -> return True
        (Just hc, handles) -> do
          case Map.member h handles of
            False -> return True
            True  -> loopClientUntil client h (Just $ n - 1) v 
    False -> return True

loopClientUntil client h (Nothing) v = do
  empty <- isEmptyMVar v
  case empty of
    True -> do
      loopResult <- loopClient client
      clientData <- readMVar $ unClient client
      --  TODO: Exponential backoff or some other approach for polling
      case clientData of
        (Nothing, _)       -> return False
        (Just hc, handles) -> do
          case Map.member h handles of
            False -> return True
            True  -> loopClientUntil client h (Nothing) v 
    False -> return True

peekMVar :: MVar a -> IO (Maybe a)
peekMVar m = do
  res <- tryTakeMVar m
  case res of
    Just r  -> do
      putMVar m r
      return $ Just r
    Nothing -> return $ Nothing

promise :: Client -> Handle -> MVar (Either HyperclientReturnCode a) -> IO (Either HyperclientReturnCode a)
promise client h v | h > 0 = do
                          loopResult <- loopClientUntil client h Nothing v 
                          res <- peekMVar v
                          return $ fromMaybe (error "This should not occur!") res

withClientImmediate :: Client -> (Hyperclient -> IO a) -> IO a
withClientImmediate (unClient -> c) f =
  withMVar c $ \value -> do
    case value of
      (Nothing, _) -> error "HyperDex client error - cannot use a closed connection."
      (Just hc, _) -> f hc

withClient :: Client -> (Hyperclient -> Result a) -> IO (IO (Either HyperclientReturnCode a))
withClient client@(unClient -> c) f = do
  value <- takeMVar c
  case value of
    (Nothing, _)        -> error "HyperDex client error - cannot use a closed connection."
    (Just hc, handles)  -> do
      (h, cont) <- f hc
      case h > 0 of
        True  -> do
          v <- newEmptyMVar :: IO (MVar (Either HyperclientReturnCode a))
          putMVar c (Just hc, Map.insert h (cont >>= putMVar v) handles)
          return $ do
            loopResult <- loopClientUntil client h Nothing v 
            res <- peekMVar v
            return $ fromMaybe (error "This should not occur!") res
        False -> do
          putMVar c (Just hc, handles)
          return cont

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
