{-# LANGUAGE TypeFamilies, FlexibleContexts, BangPatterns, RecordWildCards #-}
{-# LANGUAGE ForeignFunctionInterface #-}
-- For writing code this is really nice:
{-# LANGUAGE TypeHoles #-}

-- |
-- Module       : Database.HyperDex.Internal.Core
-- Copyright    : (c) Aaron Friel 2014
-- License      : BSD-style
-- Maintainer   : mayreply@aaronfriel.com
-- Stability    : unstable
-- Portability  : portable
--
module Database.HyperDex.Internal.Core
  ( BusyBee (..)
  , ReturnCodeType (..)
  , Connection
  , connect
  , disconnect
  , run
  , wrap
  , wrapStream
  )
  where

import Database.HyperDex.Internal.Options
import Database.HyperDex.Internal.Handle
import qualified Database.HyperDex.Internal.Handle as HandleMap
import Database.HyperDex.Internal.Result
import Database.HyperDex.Exception

import Control.Concurrent
import Control.Exception
import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import Data.ByteString.Unsafe
import Foreign
import Foreign.C

-- | A generalization over the two types of HyperDex connections, admin and
-- client.
-- 
-- The terminology here may not be precisely correct - but this is effectively
-- an interface to the BusyBee event loop that is wrapped around in the HyperDex
-- client library.
class (Eq (ReturnCode o), Enum (ReturnCode o)) => BusyBee o where
  data ConnectionWrapper o :: *
  data ImplOptions o :: *
  data ReturnCode o :: *

  getConnection :: ConnectionWrapper o -> Connection

  getOptions :: ConnectionWrapper o -> ImplOptions o

  returnCodeType :: ReturnCode o -> ReturnCodeType

  -- | Create a connection.
  create :: ImplOptions o
         -- ^ Implementation specific options
         -> CString
         -- ^ The hostname or IP address of the coordinator.
         -> CUShort
         -- ^ The port number. 
         -> IO (Ptr o)
         -- ^ The wrapped opaque type.

  -- | Destroy the connection.
  destroy :: ImplOptions o
          -- ^ Implementation specific options
          -> Ptr o
          -- ^ Pointer to opaque structure
          -> IO ()

  -- | Poll the event loop for 'Int' milliseconds.
  loop :: ImplOptions o
       -- ^ Implementation specific options
       -> Ptr o
       -- ^ A pointer to opaque structure
       -> CInt
       -- ^ The time to delay, typically in microseconds.
       -> IO (Handle, ReturnCode o)

  -- | Construct the wrapper given the associated data type and the connection
  -- object.
  ctor :: ImplOptions o
       -- ^ Implementation specific options.
       -> Connection
       -- ^ Connection object created by this module.
       -> ConnectionWrapper o

  -- | This event handler will be called whenever a new asynchronous operation
  -- is added to the event loop.
  --
  -- This will always be called asynchronously. It is safe to access the
  -- connection within this handler.
  onAsyncStart :: ConnectionWrapper o -> IO ()
  onAsyncStart = const $ return ()

  -- | This event handler will be called whenever an asynchronous operation has
  -- been completed.
  --
  -- Note: The event loop will not call into Haskell and cause this to fire.
  -- Rather, this event handler is called if and only if the wrapper around
  -- 'loop', 'run', is called and the callback will subsequently ensure that
  -- 
  -- After all pending operations have been completed, this event handler will
  -- have been called exactly as many times as 'onAsyncStart'.
  --
  -- This will always be called asynchronously. It is safe to access the
  -- connection within this handler.
  onAsyncFinish :: ConnectionWrapper o -> IO ()
  onAsyncFinish = const $ return ()

  -- | This event handler will be called when the queue of pending operations is
  -- empty.
  --
  -- Note: As above with 'onAsyncFinish', the event loop will not call into
  -- Haskell and cause this to fire. This event handler will only be called
  -- after 'run' is used to clear all pending operations from the event loop.
  --
  -- This will always be called asynchronously. It is safe to access the
  -- connection within this handler.
  onAsyncNonePending :: ConnectionWrapper o -> IO ()
  onAsyncNonePending = const $ return ()

  -- | This event handler will be called when the result of a pending operation 
  -- is being synchronously demanded, i.e.: a thread is blocked on the return
  -- value.
  --
  -- The 'IO Bool' parameter is the block checking parameter and returns:
  --
  --   * 'True' when a thread is still blocked.
  --   
  --   * 'False' when the blocking condition is alleviated.
  --  
  -- It is the responsibility of an implementation for this function to recurse
  -- and pass the blocked-checking parameter.
  --
  -- Default implementation: runs the block check, and if it succeeds, call
  -- 'run' and then recursively loop.
  -- 
  -- This will always be called asynchronously. It is safe to access the
  -- connection within this handler.
  onAsyncBlocking :: ConnectionWrapper o -> IO Bool -> IO ()
  onAsyncBlocking wrapper check  = do
    blocked <- check
    case blocked of
      False -> return ()
      True -> do
        run wrapper 0
        onAsyncBlocking wrapper check

foreign import ccall "wrapper"
  wrapFinalizer :: (Ptr o -> IO ()) -> IO (FinalizerPtr o)

data ReturnCodeType = ReturnCodeSuccess
                    | ReturnCodeNonepending
                    | ReturnCodeTransientError
                    | ReturnCodeFatalError
  deriving (Show, Eq)

-- | Wrapper around connections carrying the information for the connection 
-- along with the internal opaque object and a lock that must be used to access
-- the mapping of handles to actions.
data Connection = Connection
  { internals_  :: {-# UNPACK #-} !(MVar SynchronizedData)
  , pool_       :: {-# UNPACK #-} !Pool
  , info_       :: {-# UNPACK #-} !ConnectInfo
  }

-- | Strict and as unpacked as possible connection implementation.
data SynchronizedData = SynchronizedData
  { ptr_    :: {-# UNPACK #-} !(ForeignPtr ())
  , closed_ :: !Bool
  , map_    :: !HandleMap
  }

-- | Safely modify the 'SynchronizedData' inside a 'ConnectionWrapper o'
--
-- This will obtain a lock and is not reentry safe.
-- 
-- Throws 'ClosedConnectionException' if the connection is closed.
safeModifySynchronized_ :: BusyBee o
                        => ConnectionWrapper o
                        -> (SynchronizedData -> IO SynchronizedData)
                        -> IO ()
safeModifySynchronized_ wrapper f = do
  let Connection {..} = getConnection wrapper
  modifyMVar_ internals_ $ \sync@(SynchronizedData {..}) -> do
    case closed_ of
      True  -> throwIO ClosedConnectionException
      False -> f sync

-- | Safely modify the 'SynchronizedData' inside a 'ConnectionWrapper o'
--
-- This will obtain a lock and is not reentry safe.
-- 
-- Throws 'ClosedConnectionException' if the connection is closed.
safeModifyHandles_ :: BusyBee o
                   => ConnectionWrapper o
                   -> (HandleMap -> HandleMap)
                   -> IO ()
safeModifyHandles_ wrapper f = do
  safeModifySynchronized_ wrapper $ \sync@(SynchronizedData {..}) ->
    return $ sync { map_ = f map_ }

addHandleCallback :: BusyBee o
                  => ConnectionWrapper o
                  -> Handle
                  -> HandleCallback
                  -> IO ()
addHandleCallback wrapper handle handleCallback = do
  _ <- forkIO $ do
    safeModifyHandles_ wrapper $ HandleMap.insert handle handleCallback
    onAsyncStart wrapper
  return ()

removeHandleCallback :: BusyBee o
                     => ConnectionWrapper o
                     -> Handle
                     -> IO ()
removeHandleCallback wrapper handle = do
  _ <- forkIO $ do
    safeModifyHandles_ wrapper $ HandleMap.delete handle
    onAsyncFinish wrapper
  return ()

-- | Safely access the 'SynchronizedData' inside a 'ConnectionWrapper o'
--
-- This will obtain a lock and is not reentry safe.
-- 
-- Throws 'ClosedConnectionException' if the connection is closed.
safeWithSynchronized :: BusyBee o
                     => ConnectionWrapper o
                     -> (SynchronizedData -> IO ())
                     -> IO ()
safeWithSynchronized wrapper f = do
  let Connection {..} = getConnection wrapper
  withMVar internals_ $ \sync@(SynchronizedData {..}) -> do
    case closed_ of
      True  -> throwIO ClosedConnectionException
      False -> f sync

-- | Cast the 'ForeignPtr' to a locally usable value.
--
-- It is never safe for the function supplied to return the pointer passed to it
-- under any condition.
withSyncPtr :: ForeignPtr () -> (Ptr a -> IO b) -> IO b
withSyncPtr fPtr f = withForeignPtr fPtr (f . castPtr) 

-- | Create a connection and return a 'ConnectionWrapper o'
--
-- Throws 'ClosedConnectionException' if the connection is closed.
connect :: BusyBee o
        => ImplOptions o
        -> ConnectInfo
        -> IO (ConnectionWrapper o)
connect !opt !info@(ConnectInfo {..}) = do
  -- See Note: [Coordinator lifetime]
  pool <- newPool
  cstr <- poolAllocCBString pool connectHost
  opaquePtr <- create opt cstr (CUShort connectPort)
  finalizer <- wrapFinalizer $ \_ -> do
    -- Note: impl. of 'disconnect' depends strongly on correctness of finalizer.
    destroy opt opaquePtr
    freePool pool
  foreignPtr <- newForeignPtr finalizer opaquePtr
  synchronized <- newMVar $ 
    SynchronizedData (castForeignPtr foreignPtr) False HandleMap.empty 
  return $ ctor opt $ Connection synchronized pool info

-- | Close and deallocate the resources for a connection.
--
-- Throws 'ClosedConnectionException' if the connection is closed.
disconnect :: BusyBee o
           => ConnectionWrapper o
           -> IO ()
disconnect wrapper = 
  safeModifySynchronized_ wrapper $ \(SynchronizedData {..}) -> do 
    -- Fork off a thread to end all pending transactions.
    forkIO $ do     
      mapM_ callbackCleanup $ HandleMap.elems map_
      -- Runs 'destroy' and then runs 'freePool' on the memory pool due to impl.
      -- of 'connect'
      finalizeForeignPtr ptr_
    -- Put back a nulled, closed, empty connection value.
    nullForeignPtr <- newForeignPtr_ nullPtr
    return $ SynchronizedData nullForeignPtr True HandleMap.empty

-- | Run the event loop once for a length determed by an input timeout.
--
-- Throws 'LoopInconsistentException' if an inconsistency is detected in the event
-- loop.
--
-- Throws 'LoopFatalError' if the return code is of type 'ReturnCodeFatalError'.
--
-- Transient errors are ignored.
run :: BusyBee o
    => ConnectionWrapper o
    -> Int32
    -> IO ()
run wrapper timeout = do
  let opts = getOptions wrapper
  safeWithSynchronized wrapper $ \(SynchronizedData {..}) -> do
    (h, returnCode) <- withSyncPtr ptr_ $ \ptr -> loop opts ptr (CInt timeout)
    -- See Note: [Loop errors]
    case returnCodeType returnCode of
      
      ReturnCodeSuccess -> do
        case HandleMap.lookup h map_ of
          Nothing       -> throwIO $ HyhacMissingHandle h
          Just callback -> asyncRunCallback callback
      
      ReturnCodeTransientError -> return ()
      
      ReturnCodeFatalError     -> throwIO $ LoopFatalError (fromEnum returnCode)
      
      ReturnCodeNonepending -> do
        _ <- forkIO $ onAsyncNonePending wrapper
        case HandleMap.null map_ of
          True  -> return ()
          False -> throwIO $ HyperDexNonePending (HandleMap.keys map_)

{- Note: [Loop errors]
  
  ReturnCodeSuccess:

  If there is a success but the map does not contain the handle returned, is
  this an error? Almost certainly yes, but definitely also a TODO:

  ReturnCodeTransientError includes: _TIMEOUT and _INTERRUPTED

  ReturnCodeFatalError includes: _POLLFAILED, _ADDFDFAIL, _SHUTDOWN
-}

-- | Run a 'HandleCallback' continuation in a new thread.
--
-- This is done asynchronously to ensure that the callback cannot be reentrant
-- and deadlock on the connection.
--
-- If the callback completes and returns a new handle and callback, it will be
-- added to the 'HandleMap' of the owning connection.
asyncRunCallback :: HandleCallback
                 -> IO ()
asyncRunCallback (HandleCallback {..}) = do
  _ <- forkIO $ do
    mask $ \restore -> restore callbackContinuation 
                         `onException` callbackCleanup 
  return ()

-- UTILITY FUNCTIONS

-- | Unsafe impl. of allocating a null-terminated ByteString within a 'Pool'
poolAllocCBString :: Pool -> ByteString -> IO CString
poolAllocCBString pool bs = 
  unsafeUseAsCString bs $ \cs -> do
    let l = ByteString.length bs
    buf <- pooledMallocArray0 pool l :: IO (Ptr CChar)
    copyBytes buf cs l
    pokeElemOff buf l 0
    return buf

-- | Creates a pair of MVars to represent a pending operation's status.
--
-- Internally, two 'MVar's are created:
--
--   * The first begins with the value 'True' and will hold this value until
--     the operation is complete.
--
--   * The second begins empty and will store the completed result.
--
-- The three functions returned are:
--
--   * A check to return the value inside the first 'MVar'.
--
--   * A function to provide a value to the second empty 'MVar', setting its
--     value and flipping the first 'MVar' to 'True'. 
--
--   * A function to take the value.
--
-- Double puts are prevented by only allowing the first put and discarding
-- subsequent puts. Double gets are safe because the take will always put back
-- its value. All access is gated by the first 'MVar' to ensure all puts and
-- gets are sequentially ordered.
--
newBlockingAsyncValue :: IO (IO Bool, a -> IO (), IO a)
newBlockingAsyncValue = do
  blocking <- newMVar True
  asyncValue <- newEmptyMVar
  let checkBlocking = readMVar blocking

  let putValue = \value -> do
        modifyMVar_ blocking $ \block -> 
          case block of
            True -> do
              putMVar asyncValue value
              return False
            False -> 
              return False 
  
  let getValue = withMVar blocking $ const $ readMVar asyncValue

  return (checkBlocking, putValue, getValue)

wrap :: BusyBee o
     => ConnectionWrapper o
     -> (Ptr o -> AsyncResultHandle a)
     -> AsyncResult a
wrap wrapper f = do
  (checkBlocking, putValue, getValue) <- newBlockingAsyncValue
  safeWithSynchronized wrapper $ \(SynchronizedData {..}) -> do
    (handle, cleanup, onFinishCallback) <- withSyncPtr ptr_ f
    case handleSuccess handle of
      True -> do
        addHandleCallback wrapper handle $
          HandleCallback cleanup $ do
            removeHandleCallback wrapper handle
            onFinishCallback >>= putValue
            cleanup
      False -> do
        result <- onFinishCallback
        putValue result
  return $ onAsyncBlocking wrapper checkBlocking >> getValue

wrapStream :: BusyBee o
           => ConnectionWrapper o
           -> (Ptr o -> AsyncResultHandle a)
           -> IO (SearchStream a)
wrapStream wrapper f = do
  (checkBlocking, putValue, getValue) <- newBlockingAsyncValue
  safeWithSynchronized wrapper $ \(SynchronizedData {..}) -> do
    (handle, cleanup, onFinishCallback) <- withSyncPtr ptr_ f
    case handleSuccess handle of
      True -> do
        -- This is a fixed point whose parameter is the preceeding 'put'
        -- function. Recursively, each new either terminates returning Left _
        -- or creates a newBlockingAsyncValue and passes the putValue to itself.
        let callbackCont streamPut = do
              result <- onFinishCallback
              case result of
                Left returnCode -> do
                  removeHandleCallback wrapper handle
                  streamPut $ Left returnCode
                  cleanup
                Right value -> do
                  (streamBlock', streamPut', streamGet') <- newBlockingAsyncValue
                  let fixed = do
                        callbackCont streamPut'
                        -- notify that we are about to block on the current value
                        onAsyncBlocking wrapper streamBlock'
                        -- get the current value
                        streamGet'
                  streamPut $ Right (value, SearchStream fixed)
        addHandleCallback wrapper handle $
          HandleCallback cleanup (callbackCont putValue)
      False -> do
        result <- onFinishCallback
        case result of
          Left returnCode -> putValue $ Left returnCode
          Right _ -> throwIO $ SearchResponseUnexpected
  return $ SearchStream (onAsyncBlocking wrapper checkBlocking >> getValue)

{-

  Note [Coordinator lifetime]:

  Sharing values with foreign libraries is more performant than copying, but is
  technically complicated. This implementation guarantees via the 'Pool' that
  allocations within the pool will be freed when the connection is garbage
  collected. Guaranteeing that pool allocations are released in a timely way
  otherwise is more complex, and up to implementation detail.

  Note [TransientErrorRetry]:

  Previous implementations treated transient errors, particularly from within
  the prior 'withClient' implementations, as errors that should be transparently
  retried. This is probably incorrect behavior, but it needs to be checked with
  the HyperDex developers. It is at the very least more desirable that transient
  errors do not cause unexpected behavior - i.e.: duplicated operations.

  Behavior change: 0.11

-}
