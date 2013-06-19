{-# LANGUAGE ViewPatterns #-}

module Database.HyperDex.Internal.Client 
  ( Hyperclient, Client
  , ConnectInfo (..)
  , defaultConnectInfo
  , ConnectOptions (..)
  , defaultConnectOptions
  , BackoffMethod (..)
  , Handle
  , Result, AsyncResult, AsyncResultHandle, SearchStream (..)
  , connect, close
  , loopClient, loopClientUntil
  , withClient, withClientImmediate
  , withClientStream
  )
  where

{# import Database.HyperDex.Internal.ReturnCode #}
import Database.HyperDex.Internal.Util

import Data.Map (Map)
import qualified Data.Map as Map

import Control.Concurrent (yield, threadDelay)
import Control.Concurrent.MVar

import Data.Word (Word16)

import Data.Text.Encoding (encodeUtf8)
import qualified Data.Text as Text (pack) 

import Data.Default

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

-- | A callback used to perform work when the Hyperclient loop indicates an
-- operation has been completed.
--
-- A 'Nothing' value indicates that no further work is necessary, and a 'Just' value
-- will store a new Handle and HandleCallback.
newtype HandleCallback = HandleCallback (Maybe ReturnCode -> IO (Maybe (Handle, HandleCallback)))

-- | The core data type managing access to a 'Hyperclient' object and all
-- currently running asynchronous operations.
--
-- The 'MVar' is used as a lock to control access to the 'Hyperclient' and
-- a map of open handles and continuations, or callbacks, that must be executed
-- to complete operations. A 'HandleCallback' may yield Nothing or a new 'Handle'
-- and 'HandleCallback' to be stored in the map.
type HyperclientWrapper = MVar (Maybe Hyperclient, Map Handle HandleCallback)

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

-- | A return value used internally by HyperClient operations.
--
-- This is the same as 'AsyncResultHandle' except it gives the callback
-- the result of the loop operation that yields the returned 'Handle'.
type StreamResultHandle a = IO (Handle, Maybe ReturnCode -> Result a)

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

newtype SearchStream a = SearchStream (a, Result (SearchStream a))

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
      mapM_ (\(HandleCallback cont) -> cont Nothing) $ Map.elems handles 
      putMVar c (Nothing, Map.empty)

doExponentialBackoff :: Int -> Double -> (Int, BackoffMethod)
doExponentialBackoff b x = 
  let result = ceiling (fromIntegral b ** x) in
    (result, BackoffExponential result x)
{-# INLINE doExponentialBackoff #-}

cappedBackoff :: Int -> Maybe Int -> (Int, Bool)
cappedBackoff n Nothing               = (n, False)
cappedBackoff n (Just c) | n  < c     = (n, False)
                         | otherwise  = (c, True)
{-# INLINE cappedBackoff #-}

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
{-# INLINE performBackoff #-}

-- | Runs hyperclient_loop exactly once, setting the appropriate MVar.
loopClient :: Client -> IO (Maybe Handle)
loopClient client@(getClient -> c) = do
  clientData <- takeMVar c
  case clientData of
    (Nothing, _)        -> return Nothing
    (Just hc, handles)  -> do
      -- TODO: Examine returnCode for things that might matter.
      (handle, returnCode) <- hyperclientLoop hc 0
      case returnCode of
        HyperclientSuccess -> do
          let newMap = Map.delete handle handles
          case Map.lookup handle handles of
            Just (HandleCallback entry) -> do
              cont <- entry $ Just returnCode
              case cont of
                Nothing -> putMVar c (Just hc, newMap)
                Just (h, e) -> putMVar c (Just hc, Map.insert h e newMap)
            Nothing -> putMVar c (Just hc, newMap)
          return $ Just handle
        HyperclientTimeout -> do
          putMVar c (Just hc, handles)
          loopClient client
        HyperclientNonepending -> do
          mapM_ (\(HandleCallback cont) -> cont Nothing) $ Map.elems handles
          putMVar c (Just hc, Map.empty)
          return $ Just handle
        _ -> do
          putMVar c (Just hc, handles)
          loopClient client
{-# INLINE loopClient #-}

-- | Run hyperclient_loop at most N times or forever until a handle
-- is returned.
loopClientUntil :: Client -> Handle -> MVar a -> BackoffMethod -> Maybe Int -> IO (Bool)
loopClientUntil _      _ _ _    (Just 0) = return False

loopClientUntil client h v back (Just n) = do
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
              loopClientUntil client h v back' (Just $ n - 1)
    False -> return True

loopClientUntil client h v back Nothing = do
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
              loopClientUntil client h v back' Nothing
    False -> return True
{-# INLINE loopClientUntil #-}

-- | Wrap a HyperClient request and wait until completion or failure.
withClientImmediate :: Client -> (Hyperclient -> IO a) -> IO a
withClientImmediate (getClient -> c) f =
  withMVar c $ \value -> do
    case value of
      (Nothing, _) -> error "HyperDex client error - cannot use a closed connection."
      (Just hc, _) -> f hc
{-# INLINE withClientImmediate #-}

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
          let wrappedCallback = HandleCallback $ const $ do
                returnValue <- cont
                putMVar v returnValue
                return Nothing
          putMVar c (Just hc, Map.insert h wrappedCallback handles)
          return $ do
            success <- loopClientUntil client h v (connectionBackoff . getConnectOptions $ client) Nothing 
            case success of
              True  -> takeMVar v
              False -> return $ Left HyperclientPollfailed
        False -> do
          putMVar c (Just hc, handles)
          returnValue <- cont
          -- A HyperclientInterrupted return code indicates that there was a signal
          -- received by the client that prevented the call from completing, thus
          -- the request should be transparently retried.
          case returnValue of
            Left HyperclientInterrupted -> withClient client f
            _ -> return . return $ returnValue
{-# INLINE withClient #-}

-- | Wrap a Hyperclient request that returns a search stream.
withClientStream :: Client -> (Hyperclient -> StreamResultHandle a) -> AsyncResult (SearchStream a)
withClientStream client@(getClient -> c) f = do
  value <- takeMVar c
  case value of
    (Nothing, _)        -> error "HyperDex client error - cannot use a closed connection."
    (Just hc, handles)  -> do
      (h, cont) <- f hc
      case h > 0 of
        True  -> do
          v <- newEmptyMVar :: IO (MVar (Either ReturnCode (SearchStream a)))
          let wrappedCallback = HandleCallback $ \code -> do
                returnValue <- cont code
                (result, callback) <- wrapSearchStream returnValue client h cont
                putMVar v $ result
                return $ Just (h, callback)
          putMVar c (Just hc, Map.insert h wrappedCallback handles)
          return $ do
            success <- loopClientUntil client h v (connectionBackoff . getConnectOptions $ client) Nothing 
            case success of
              True  -> takeMVar v
              False -> return $ Left HyperclientPollfailed
        False -> do
          putMVar c (Just hc, handles)
          returnValue <- cont Nothing
          case returnValue of
            Left HyperclientInterrupted -> withClientStream client f
            _ -> do
              (result, _) <- wrapSearchStream returnValue client h cont
              return . return $ result
{-# INLINE withClientStream #-}

wrapSearchStream :: Either ReturnCode a -> Client -> Handle -> (Maybe ReturnCode -> Result a) -> IO (Either ReturnCode (SearchStream a), HandleCallback)
wrapSearchStream (Left e)  _      _ _    = return $ (Left e, HandleCallback $ const $ return Nothing)
wrapSearchStream (Right a) client h cont = do
  v <- newEmptyMVar
  let wrappedCallback = HandleCallback $ \code -> do
        returnValue <- cont code
        (result, callback) <- wrapSearchStream returnValue client h cont
        putMVar v $ result
        return $ Just (h, callback)
  let cont' = do
        success <- loopClientUntil client h v (connectionBackoff . getConnectOptions $ client) Nothing
        case success of
          True  -> takeMVar v
                  -- TODO: Return actual ReturnCode
          False -> return $ Left HyperclientPollfailed
  return $ (return $ SearchStream (a, cont'), wrappedCallback)
{-# INLINE wrapSearchStream #-}

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
