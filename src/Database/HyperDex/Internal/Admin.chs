{-# LANGUAGE ViewPatterns #-}

module Database.HyperDex.Internal.Admin
  -- TODO exports
  where

import Foreign
import Foreign.C

import Data.ByteString (ByteString, useAsCString)

{# import Database.HyperDex.Internal.AdminReturnCode #}
import Database.HyperDex.Internal.Util

import Data.Map (Map)
import qualified Data.Map as Map

import Control.Concurrent (yield, threadDelay)
import Control.Concurrent.MVar

import Data.Text.Encoding (encodeUtf8)
import qualified Data.Text as Text (pack)

import Data.Default

#include "hyperdex/admin.h"

{#pointer *hyperdex_admin as HyperdexAdmin #}

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
      { connectionBackoff = BackoffYield -- 10 * 2^n
      , connectionBackoffCap = Just 50000           -- 0.05 seconds.
      }

-- | Sane defaults for HyperDex connection options.
defaultConnectOptions :: ConnectOptions
defaultConnectOptions = def

-- | A connectionBackoff method controls how frequently the admin polls internally.
--
-- This is provided to allow fine-tuning performance. Do note that
-- this does not affect any method the HyperdexAdmin C library uses to poll
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

-- | A callback used to perform work when the HyperdexAdmin loop indicates an
-- operation has been completed.
--
-- A 'Nothing' value indicates that no further work is necessary, and a 'Just' value
-- will store a new Handle and HandleCallback.
newtype HandleCallback = HandleCallback (Maybe ReturnCode -> IO (Maybe (Handle, HandleCallback)))

-- | The core data type managing access to a 'HyperdexAdmin' object and all
-- currently running asynchronous operations.
--
-- The 'MVar' is used as a lock to control access to the 'HyperdexAdmin' and
-- a map of open handles and continuations, or callbacks, that must be executed
-- to complete operations. A 'HandleCallback' may yield Nothing or a new 'Handle'
-- and 'HandleCallback' to be stored in the map.
type HyperdexAdminWrapper = MVar (Maybe HyperdexAdmin, Map Handle HandleCallback)

data AdminData =
  AdminData
    { hyperdexAdminWrapper :: HyperdexAdminWrapper
    , connectionInfo     :: ConnectInfo
    }

-- | A connection to a HyperDex cluster.
newtype Admin = Admin { unAdminData :: AdminData }

-- | Internal method for returning the (MVar) wrapped connection.
getAdmin :: Admin -> HyperdexAdminWrapper
getAdmin = hyperdexAdminWrapper . unAdminData

-- | Get the connection info used for a 'Admin'.
getConnectInfo :: Admin -> ConnectInfo
getConnectInfo = connectionInfo . unAdminData

-- | Get the connection options for a 'Admin'.
getConnectOptions :: Admin -> ConnectOptions
getConnectOptions = connectOptions . getConnectInfo

-- | Return value from hyperdex_admin operations.
--
-- Per the specification, it's guaranteed to be a unique integer for
-- each outstanding operation using a given HyperdexAdmin. In practice
-- it is monotonically increasing while operations are outstanding,
-- lower values are used first, and negative values represent an
-- error.
type Handle = {# type int64_t #}

-- | A return value from HyperDex.
type Result a = IO (Either ReturnCode a)

-- | A return value used internally by HyperdexAdmin operations.
--
-- Internally the wrappers to the HyperDex library will return
-- a computation that yields a 'Handle' referring to that request
-- and a continuation that will force the request to return an
-- error in the form of a ReturnCode or a result.
--
-- The result of forcing the result is undefined.
-- The HyperdexAdmin and its workings are not party to the MVar locking
-- mechanism, and the ReturnCode and/or return value may be in the
-- process of being modified when the computation is forced.
--
-- Consequently, the only safe way to use this is with a wrapper such
-- as 'withAdmin', which only allows the continuation to be run after
-- the HyperdexAdmin has returned the corresponding Handle or after the
-- HyperdexAdmin has been destroyed.
type AsyncResultHandle a = IO (Handle, Result a)

-- | A return value used internally by HyperdexAdmin operations.
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
connect :: ConnectInfo -> IO Admin
connect info = do
  hyperdex_admin <- hyperdexAdminCreate (encodeUtf8 . Text.pack . connectHost $ info) (connectPort info)
  adminData <- newMVar (Just hyperdex_admin, Map.empty)
  return $
    Admin
    $ AdminData
      { hyperdexAdminWrapper = adminData
      , connectionInfo = info
      }

-- | Close a connection and terminate any outstanding asynchronous
-- requests.
--
-- /Note:/ This does force all asynchronous requests to complete
-- immediately. Any outstanding requests at the time the 'Admin'
-- is closed ought to return a 'ReturnCode' indicating the failure
-- condition, but the behavior is ultimately undefined. Any pending
-- requests should be disregarded.
forceClose :: Admin -> IO ()
forceClose (getAdmin -> c) = do
  adminData <- takeMVar c
  case adminData of
    (Nothing, _)        -> error "HyperDex admin error - cannot close a admin connection twice."
    (Just hc, handles)  -> do
      hyperdexAdminDestroy hc
      mapM_ (\(HandleCallback cont) -> cont Nothing) $ Map.elems handles
      putMVar c (Nothing, Map.empty)

-- | Wait for graceful termination of all outstanding requests and
-- then close the connection.
--
-- /Note:/ If it is necessary to have this operation complete quickly
-- and outstanding requests are not needed, then use 'forceClose'.
close :: Admin -> IO ()
close admin@(getAdmin -> c) = do
  adminData <- takeMVar c
  case adminData of
    (Nothing, _)        -> error "HyperDex admin error - cannot close a admin connection twice."
    (Just hc, handles)  -> do
      case Map.null handles of
        True  -> do
          hyperdexAdminDestroy hc
          putMVar c (Nothing, Map.empty)
        False -> do
          -- Have to put it back in order to run loopAdmin
          (_, newHandles) <- handleLoop hc handles
          putMVar c (Just hc, newHandles)
          close admin

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

-- | Runs a single iteration of hyperdex_admin_loop, returning whether
-- or not a handle was completed and a new set of callbacks.
--
-- This function does not use locking around the admin.
handleLoop :: HyperdexAdmin -> Map Handle HandleCallback -> IO (Maybe Handle, Map Handle HandleCallback)
handleLoop hc handles = do
  -- TODO: Examine returnCode for things that might matter.
  (handle, returnCode) <- hyperdexAdminLoop hc 0
  case returnCode of
    HyperdexAdminSuccess -> do
      let clearedMap = Map.delete handle handles
      resultMap <- do
        case Map.lookup handle handles of
          Just (HandleCallback entry) -> do
            cont <- entry $ Just returnCode
            case cont of
              Nothing     -> return clearedMap
              Just (h, e) -> return $ Map.insert h e clearedMap
          Nothing -> return clearedMap
      return $ (Just handle, resultMap)
    HyperdexAdminTimeout -> do
      handleLoop hc handles
    HyperdexAdminNonepending -> do
      mapM_ (\(HandleCallback cont) -> cont Nothing) $ Map.elems handles
      return $ (Just handle, Map.empty)
    _ -> do
      handleLoop hc handles

{-# INLINE handleLoop #-}

-- | Runs hyperdex_admin_loop exactly once, setting the appropriate MVar.
loopAdmin :: Admin -> IO (Maybe Handle)
loopAdmin (getAdmin -> c) = do
  adminData <- takeMVar c
  case adminData of
    (Nothing, _)       -> error "HyperDex admin error - admin has been closed."
    (Just hc, handles) -> do
      (maybeHandle, newHandles) <- handleLoop hc handles
      putMVar c (Just hc, newHandles)
      return maybeHandle
{-# INLINE loopAdmin #-}

-- | Run hyperdex_admin_loop at most N times or forever until a handle
-- is returned.
loopAdminUntil :: Admin -> Handle -> MVar a -> BackoffMethod -> Maybe Int -> IO (Bool)
loopAdminUntil _      _ _ _    (Just 0) = return False

loopAdminUntil admin h v back (Just n) = do
  empty <- isEmptyMVar v
  case empty of
    True -> do
      _ <- loopAdmin admin
      adminData <- readMVar $ getAdmin admin
      --  TODO: Exponential connectionBackoff or some other approach for polling
      case adminData of
        (Nothing, _)       -> return True
        (Just _, handles)  -> do
          case Map.member h handles of
            False -> return True
            True  -> do
              back' <- performBackoff back (connectionBackoffCap . getConnectOptions $ admin)
              loopAdminUntil admin h v back' (Just $ n - 1)
    False -> return True

loopAdminUntil admin h v back Nothing = do
  empty <- isEmptyMVar v
  case empty of
    True -> do
      _ <- loopAdmin admin
      adminData <- readMVar $ getAdmin admin
      --  TODO: Exponential connectionBackoff or some other approach for polling
      case adminData of
        (Nothing, _)       -> return False
        (Just _, handles)  -> do
          case Map.member h handles of
            False -> return True
            True  -> do
              back' <- performBackoff back (connectionBackoffCap . getConnectOptions $ admin)
              loopAdminUntil admin h v back' Nothing
    False -> return True
{-# INLINE loopAdminUntil #-}

-- | Wrap a HyperdexAdmin request and wait until completion or failure.
withAdminImmediate :: Admin -> (HyperdexAdmin -> IO a) -> IO a
withAdminImmediate (getAdmin -> c) f =
  withMVar c $ \value -> do
    case value of
      (Nothing, _) -> error "HyperDex admin error - cannot use a closed connection."
      (Just hc, _) -> f hc
{-# INLINE withAdminImmediate #-}

-- | Wrap a HyperdexAdmin request.
withAdmin :: Admin -> (HyperdexAdmin -> AsyncResultHandle a) -> AsyncResult a
withAdmin admin@(getAdmin -> c) f = do
  value <- takeMVar c
  case value of
    (Nothing, _)        -> error "HyperDex admin error - cannot use a closed connection."
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
            success <- loopAdminUntil admin h v (connectionBackoff . getConnectOptions $ admin) Nothing
            case success of
              True  -> takeMVar v
              False -> return $ Left HyperdexAdminPollfailed
        False -> do
          putMVar c (Just hc, handles)
          returnValue <- cont
          -- A HyperdexAdminInterrupted return code indicates that there was a signal
          -- received by the admin that prevented the call from completing, thus
          -- the request should be transparently retried.
          case returnValue of
            Left HyperdexAdminInterrupted -> withAdmin admin f
            _ -> return . return $ returnValue
{-# INLINE withAdmin #-}

-- | Wrap a HyperdexAdmin request that returns a search stream.
withAdminStream :: Admin -> (HyperdexAdmin -> StreamResultHandle a) -> AsyncResult (SearchStream a)
withAdminStream admin@(getAdmin -> c) f = do
  value <- takeMVar c
  case value of
    (Nothing, _)        -> error "HyperDex admin error - cannot use a closed connection."
    (Just hc, handles)  -> do
      (h, cont) <- f hc
      case h > 0 of
        True  -> do
          v <- newEmptyMVar :: IO (MVar (Either ReturnCode (SearchStream a)))
          let wrappedCallback = HandleCallback $ \code -> do
                returnValue <- cont code
                (result, callback) <- wrapSearchStream returnValue admin h cont
                putMVar v $ result
                return $ Just (h, callback)
          putMVar c (Just hc, Map.insert h wrappedCallback handles)
          return $ do
            success <- loopAdminUntil admin h v (connectionBackoff . getConnectOptions $ admin) Nothing
            case success of
              True  -> takeMVar v
              False -> return $ Left HyperdexAdminPollfailed
        False -> do
          putMVar c (Just hc, handles)
          returnValue <- cont Nothing
          case returnValue of
            Left HyperdexAdminInterrupted -> withAdminStream admin f
            _ -> do
              (result, _) <- wrapSearchStream returnValue admin h cont
              return . return $ result
{-# INLINE withAdminStream #-}

wrapSearchStream :: Either ReturnCode a -> Admin -> Handle -> (Maybe ReturnCode -> Result a) -> IO (Either ReturnCode (SearchStream a), HandleCallback)
wrapSearchStream (Left e)  _      _ _    = return $ (Left e, HandleCallback $ const $ return Nothing)
wrapSearchStream (Right a) admin h cont = do
  v <- newEmptyMVar
  let wrappedCallback = HandleCallback $ \code -> do
        returnValue <- cont code
        (result, callback) <- wrapSearchStream returnValue admin h cont
        putMVar v $ result
        return $ Just (h, callback)
  let cont' = do
        success <- loopAdminUntil admin h v (connectionBackoff . getConnectOptions $ admin) Nothing
        case success of
          True  -> takeMVar v
                  -- TODO: Return actual ReturnCode
          False -> return $ Left HyperdexAdminPollfailed
  return $ (return $ SearchStream (a, cont'), wrappedCallback)
{-# INLINE wrapSearchStream #-}

-- | C wrapper for hyperdex_admin_create. Creates a HyperdexAdmin given a host
-- and a port.
--
-- C definition:
--
-- > struct hyperdex_admin*
-- > hyperdex_admin_create(const char* coordinator, uint16_t port);
hyperdexAdminCreate :: ByteString -> Word16 -> IO HyperdexAdmin
hyperdexAdminCreate h port = useAsCString h $ \host ->
  wrapHyperCall $ {# call hyperdex_admin_create #} host (fromIntegral port)

-- | C wrapper for hyperdex_admin_destroy. Destroys a HyperdexAdmin.
--
-- /Note:/ This does not ensure resources are freed. Any memory
-- allocated as staging for incomplete requests will not be returned.
--
-- C definition:
--
-- > void
-- > hyperdex_admin_destroy(struct hyperdex_admin* admin);
hyperdexAdminDestroy :: HyperdexAdmin -> IO ()
hyperdexAdminDestroy admin = wrapHyperCall $
  {# call hyperdex_admin_destroy #} admin

-- | C wrapper for hyperdex_admin_loop. Waits up to some number of
-- milliseconds for a result before returning.
--
-- A negative 'Handle' return value indicates a failure condition
-- or timeout, a positive value indicates completion of an asynchronous
-- request.
--
-- C definition:
--
-- > int64_t
-- > hyperdex_admin_loop(struct hyperdex_admin* admin, int timeout,
-- >                  enum hyperdex_admin_returncode* status);
hyperdexAdminLoop :: HyperdexAdmin -> Int -> IO (Handle, ReturnCode)
hyperdexAdminLoop admin timeout =
  alloca $ \returnCodePtr -> do
    handle <- wrapHyperCall $ {# call hyperdex_admin_loop #} admin (fromIntegral timeout) returnCodePtr
    returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
    return (handle, returnCode)
{-# INLINE hyperdexAdminLoop #-}
