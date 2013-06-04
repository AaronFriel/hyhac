{-# LANGUAGE ViewPatterns #-}

module Database.HyperDex.Internal.Client 
  ( Hyperclient, Client
  , ConnectInfo (..)
  , defaultConnectInfo
  , ConnectOptions (..)
  , defaultConnectOptions
  , BackoffMethod (..)
  , Handle
  , Result, AsyncResult, AsyncResultHandle
  , connect, close
  , loopClient, loopClientUntil
  , withClient, withClientImmediate
  )
  where

{# import Database.HyperDex.Internal.ReturnCode #}
import Database.HyperDex.Internal.Util

import Data.Maybe

import Data.Map (Map)
import qualified Data.Map as Map

import Control.Concurrent (yield, threadDelay)
import Control.Concurrent.MVar

import Data.Word (Word16)

import Data.Text.Encoding (encodeUtf8)
import qualified Data.Text as Text (pack) 

import Data.Default

import Debug.Trace

#include "hyperclient.h"

{#pointer *hyperclient as Hyperclient #}

-- | Parameters for connecting to a HyperDex cluster.
data ConnectInfo =
  ConnectInfo
    { connectHost :: String
    , connectPort :: Word16
    , connectOptions :: ConnectOptions
    }
  deriving (Eq, Read, Show)

instance Default ConnectInfo where
  def = 
    ConnectInfo
      { connectHost = "127.0.0.1"
      , connectPort = 1982
      , connectOptions = def
      }

defaultConnectInfo :: ConnectInfo
defaultConnectInfo = def

-- | Additional options for connecting and managing the connection
-- to a HyperDex cluster.
data ConnectOptions =
  ConnectOptions
    { connectionBackoff :: BackoffMethod
    , connectionBackoffCap :: Maybe Int
    }
  deriving (Eq, Read, Show)

instance Default ConnectOptions where
  def =
    ConnectOptions
      { connectionBackoff = BackoffExponential 10 2 -- 10 * 2^n
      , connectionBackoffCap = Just 500000          -- Half a second.
      }

-- | Sane defaults for HyperDex connection options.
defaultConnectOptions :: ConnectOptions
defaultConnectOptions = def

-- | A connectionBackoff method controls how frequently the client polls internally.
-- 
-- This is provided to allow fine-tuning performance. Do note that 
-- this does not affect any method the HyperClient C library uses to poll
-- its connection to a HyperDex cluster.
--
-- All integer values are in microseconds.
data BackoffMethod
  -- | No delay is used except the thread is yielded.
  = BackoffYield
  -- | Delay a constant number of microseconds each inter.
  | BackoffConstant Int
  -- | Delay with an initial number of microseconds, increasing linearly by the second value. 
  | BackoffLinear Int Int
  -- | Delay with an initial number of microseconds, increasing exponentially by the second value.  
  | BackoffExponential Int Double
  deriving (Eq, Read, Show)

type HyperclientWrapper = MVar (Maybe Hyperclient, Map Handle (IO ()))

data ClientData =
  ClientData
    { hyperclientWrapper :: HyperclientWrapper
    , connectionInfo     :: ConnectInfo
    }

-- | A connection to a HyperDex cluster.
newtype Client = Client { unClientData :: ClientData } 

-- | Internal method for returning the (MVar) wrapped connection. 
getClient :: Client -> HyperclientWrapper
getClient = hyperclientWrapper . unClientData

-- | Get the connection info used for a 'Client'.
getConnectInfo :: Client -> ConnectInfo
getConnectInfo = connectionInfo . unClientData

-- | Get the connection options for a 'Client'.
getConnectOptions :: Client -> ConnectOptions
getConnectOptions = connectOptions . getConnectInfo

-- | Return value from hyperclient operations.
--
-- Per the specification, it's guaranteed to be a unique integer for
-- each outstanding operation using a given HyperClient. In practice
-- it is monotonically increasing while operations are outstanding,
-- lower values are used first, and negative values represent an
-- error.
type Handle = {# type int64_t #}

-- | A return value from HyperDex.
type Result a = IO (Either ReturnCode a)

-- | A return value used internally by HyperClient operations.
--
-- Internally the wrappers to the HyperDex library will return
-- a computation that yields a 'Handle' referring to that request
-- and a continuation that will force the request to return an 
-- error in the form of a ReturnCode or a result.
--
-- The result of forcing the result is undefined.
-- The HyperClient and its workings are not party to the MVar locking
-- mechanism, and the ReturnCode and/or return value may be in the
-- process of being modified when the computation is forced.
--
-- Consequently, the only safe way to use this is with a wrapper such
-- as 'withClient', which only allows the continuation to be run after
-- the HyperClient has returned the corresponding Handle or after the
-- HyperClient has been destroyed.
type AsyncResultHandle a = IO (Handle, Result a)

-- | A return value from HyperDex in an asynchronous wrapper.
--
-- The full type is an IO (IO (Either ReturnCode a)). Evaluating
-- the result of an asynchronous call, such as the default get and
-- put operations starts the request to the HyperDex cluster. Evaluating
-- the result of that evaluation will poll internally, using the 
-- connection's 'BackoffMethod' until the result is available.
--
-- This API may be deprecated in favor of exclusively using MVars in
-- a future version.
type AsyncResult a = IO (Result a)

-- | Connect to a HyperDex cluster.
connect :: ConnectInfo -> IO Client
connect info = do
  hyperclient <- hyperclientCreate (encodeUtf8 . Text.pack . connectHost $ info) (connectPort info)
  clientData <- newMVar (Just hyperclient, Map.empty)
  return $
    Client
    $ ClientData
      { hyperclientWrapper = clientData 
      , connectionInfo = info
      }


-- | Close a connection and terminate any outstanding asynchronous
-- requests.
--
-- /Note:/ This does force all asynchronous requests to complete
-- immediately. Any outstanding requests at the time the 'Client'
-- is closed ought to return a 'ReturnCode' indicating the failure
-- condition, but the behavior is ultimately undefined. Any pending
-- requests should be disregarded. 
close :: Client -> IO ()
close (getClient -> c) = do
  clientData <- takeMVar c
  case clientData of
    (Nothing, _)        -> error "HyperDex client error - cannot close a client connection twice."
    (Just hc, handles)  -> do
      hyperclientDestroy hc
      sequence_ $ Map.elems handles 
      putMVar c (Nothing, Map.empty)

doExponentialBackoff :: Int -> Double -> (Int, BackoffMethod)
doExponentialBackoff b x = 
  let result = ceiling (fromIntegral b ** x) in
    (result, BackoffExponential result x)

cappedBackoff :: Int -> Maybe Int -> (Int, Bool)
cappedBackoff n Nothing               = (n, False)
cappedBackoff n (Just c) | n  < c     = (n, False)
                         | otherwise  = (c, True)

performBackoff :: BackoffMethod -> Maybe Int -> IO (BackoffMethod)
performBackoff method cap = do
  let (delay, newBackoff) = case method of
            BackoffYield      -> (0, method)
            BackoffConstant n -> (n, method)
            BackoffLinear m b -> (m, BackoffLinear (m+b) b)
            BackoffExponential b x -> doExponentialBackoff b x
      (backoff, capped) = cappedBackoff delay cap
  let doDelay = case backoff of
                0 -> yield
                n -> threadDelay n
      nextDelay  = case capped of
                True  -> BackoffConstant backoff
                False -> newBackoff
  doDelay >> return nextDelay

-- | Runs hyperclient_loop exactly once, setting the appropriate MVar.
loopClient' :: Bool -> Client -> IO (Maybe Handle)
loopClient' debug client@(getClient -> c) = do
  clientData <- takeMVar c
  case clientData of
    (Nothing, _)        -> return Nothing
    (Just hc, handles)  -> do
      -- TODO: Examine returnCode for things that might matter.
      (handle, returnCode) <- hyperclientLoop hc 0
      case returnCode of
        HyperclientSuccess -> do
          if debug
            then traceIO $ "Recovering from failure, returncode: " ++ show returnCode
            else return ()
          fromMaybe (return ()) $ Map.lookup handle handles
          putMVar c (Just hc, Map.delete handle handles)
          return $ Just handle
        HyperclientTimeout -> do
          putMVar c (Just hc, handles)
          loopClient' False client
        HyperclientNonepending -> do
          sequence_ $ Map.elems handles
          putMVar c (Just hc, Map.empty)
          return $ Just handle
        _ -> do
          traceIO $ "Encountered " ++ show returnCode
          putMVar c (Just hc, handles)
          loopClient' True client

loopClient :: Client -> IO (Maybe Handle)
loopClient = loopClient' False

-- | Run hyperclient_loop at most N times or forever until a handle
-- is returned.
loopClientUntil :: Client -> BackoffMethod -> Handle -> Maybe Int -> MVar (Either ReturnCode a) -> IO (Bool)
loopClientUntil _      _    _ (Just 0) _ = return False

loopClientUntil client back h (Just n) v = do
  empty <- isEmptyMVar v
  case empty of
    True -> do
      _ <- loopClient client
      clientData <- readMVar $ getClient client
      --  TODO: Exponential connectionBackoff or some other approach for polling
      case clientData of
        (Nothing, _)       -> return True
        (Just _, handles)  -> do
          case Map.member h handles of
            False -> return True
            True  -> do
              back' <- performBackoff back (connectionBackoffCap . getConnectOptions $ client)
              loopClientUntil client back' h (Just $ n - 1) v
    False -> return True

loopClientUntil client back h (Nothing) v = do
  empty <- isEmptyMVar v
  case empty of
    True -> do
      _ <- loopClient client
      clientData <- readMVar $ getClient client
      --  TODO: Exponential connectionBackoff or some other approach for polling
      case clientData of
        (Nothing, _)       -> return False
        (Just _, handles)  -> do
          case Map.member h handles of
            False -> return True
            True  -> do 
              back' <- performBackoff back (connectionBackoffCap . getConnectOptions $ client)
              loopClientUntil client back' h (Nothing) v
    False -> return True

-- | Peek at the value of an 'MVar' and return its contents if full.
-- 
-- /Note:/ This isn't truly a reliable way of performing this operation
-- as between the 'tryTakeMVar' and the 'putMVar', another thread could
-- perform the 'putMVar'. However, it is implicitly guaranteed that the
-- internal API will never have multiple-writer contention on the 
-- 'HyperclientWrapper'. Every operation performs a 'TakeMVar' before a
-- 'PutMVar', and the surface area is small enough to guarantee that.
--
-- Per 'Control.Concurrent.MVar', this function is only atomic if there
-- are no other producers for this 'MVar'.
peekMVar :: MVar a -> IO (Maybe a)
peekMVar m = do
  res <- tryTakeMVar m
  case res of
    Just r  -> do
      putMVar m r
      return $ Just r
    Nothing -> return $ Nothing

-- | Wrap a HyperClient request and wait until completion or failure.
withClientImmediate :: Client -> (Hyperclient -> IO a) -> IO a
withClientImmediate (getClient -> c) f =
  withMVar c $ \value -> do
    case value of
      (Nothing, _) -> error "HyperDex client error - cannot use a closed connection."
      (Just hc, _) -> f hc

-- | Wrap a Hyperclient request.
withClient :: Client -> (Hyperclient -> AsyncResultHandle a) -> AsyncResult a
withClient client@(getClient -> c) f = do
  value <- takeMVar c
  case value of
    (Nothing, _)        -> error "HyperDex client error - cannot use a closed connection."
    (Just hc, handles)  -> do
      (h, cont) <- f hc
      case h > 0 of
        True  -> do
          v <- newEmptyMVar :: IO (MVar (Either ReturnCode a))
          putMVar c (Just hc, Map.insert h (cont >>= putMVar v) handles)
          return $ do
            _ <- loopClientUntil client (connectionBackoff . getConnectOptions $ client) h Nothing v 
            res <- peekMVar v
            return $ fromMaybe (error "This should not occur!") res
        False -> do
          putMVar c (Just hc, handles)
          return cont

-- | C wrapper for hyperclient_create. Creates a HyperClient given a host
-- and a port.
--
-- C definition:
-- 
-- > struct hyperclient*
-- > hyperclient_create(const char* coordinator, uint16_t port);
hyperclientCreate :: ByteString -> Word16 -> IO Hyperclient
hyperclientCreate h port = withCBString h $ \host -> {# call hyperclient_create #} host (fromIntegral port)

-- | C wrapper for hyperclient_destroy. Destroys a HyperClient.
--
-- /Note:/ This does not ensure resources are freed. Any memory 
-- allocated as staging for incomplete requests will not be returned.
--
-- C definition:
-- 
-- > void
-- > hyperclient_destroy(struct hyperclient* client);
hyperclientDestroy :: Hyperclient -> IO ()
hyperclientDestroy client = do
  {# call hyperclient_destroy #} client

-- | C wrapper for hyperclient_loop. Waits up to some number of
-- milliseconds for a result before returning.
--
-- A negative 'Handle' return value indicates a failure condition
-- or timeout, a positive value indicates completion of an asynchronous
-- request.
--
-- C definition:
--
-- > int64_t
-- > hyperclient_loop(struct hyperclient* client, int timeout,
-- >                  enum hyperclient_returncode* status);
hyperclientLoop :: Hyperclient -> Int -> IO (Handle, ReturnCode)
hyperclientLoop client timeout =
  alloca $ \returnCodePtr -> do
    handle <- {# call hyperclient_loop #} client (fromIntegral timeout) returnCodePtr
    returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
    return (handle, returnCode)
