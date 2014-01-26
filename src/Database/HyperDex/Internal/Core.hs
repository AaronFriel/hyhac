{-# LANGUAGE TypeFamilies, FlexibleContexts, MultiParamTypeClasses, BangPatterns, RecordWildCards #-}
{-# LANGUAGE ForeignFunctionInterface, GeneralizedNewtypeDeriving #-}

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
  )
  where

import Database.HyperDex.Internal.Options
import Database.HyperDex.Internal.Handle
import qualified Database.HyperDex.Internal.Handle as HandleMap
-- import Database.HyperDex.Internal.Result
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

  getConnection :: ConnectionWrapper o -> IO (Connection)

  getOptions :: ConnectionWrapper o -> IO (ImplOptions o)

  returnCodeType :: ReturnCode o -> ReturnCodeType

  -- | Internal 
  create :: ImplOptions o
         -- ^ Implementation specific options
         -> CString
         -- ^ The hostname or IP address of the coordinator.
         -> CUShort
         -- ^ The port number. 
         -> IO (Ptr o)
         -- ^ The wrapped opaque type.

  -- | Disconnect, ending all pending transactions.
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

  ctor :: ImplOptions o
       -- ^ Implementation specific options.
       -> Connection
       -- ^ Connection object created by this module.
       -> ConnectionWrapper o

foreign import ccall "wrapper"
  wrapFinalizer :: (Ptr o -> IO ()) -> IO (FinalizerPtr o)

data ReturnCodeType = ReturnCodeSuccess
                    | ReturnCodeLoopTransient
                    | ReturnCodeNonepending
                    | ReturnCodeTransientError
                    | ReturnCodeFatalError

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

-- | Safely access the 'SynchronizedData' inside a 'ConnectionWrapper o'
--
-- This will obtain a lock and is not reentry safe.
-- 
-- Throws 'ClosedConnectionException' if the connection is closed.
safeWithSynchronized_ :: BusyBee o
                      => ConnectionWrapper o
                      -> (SynchronizedData -> IO SynchronizedData)
                      -> IO ()
safeWithSynchronized_ wrapper f = do
  (Connection {..}) <- getConnection wrapper
  modifyMVar_ internals_ $ \sync@(SynchronizedData {..}) -> do
    case closed_ of
      True  -> throwIO ClosedConnectionException
      False -> f sync

-- | Safely access the 'SynchronizedData' inside a 'ConnectionWrapper o'
--
-- This will obtain a lock and is not reentry safe.
-- 
-- Throws 'ClosedConnectionException' if the connection is closed.
safeWithSynchronized :: BusyBee o
                     => ConnectionWrapper o
                     -> (SynchronizedData -> IO (SynchronizedData, b))
                     -> IO b
safeWithSynchronized wrapper f = do
  (Connection {..}) <- getConnection wrapper
  modifyMVar internals_ $ \sync@(SynchronizedData {..}) -> do
    case closed_ of
      True  -> throwIO ClosedConnectionException
      False -> f sync

-- | Cast the 'ForeignPtr' to a locally usable value.
--
-- It is never safe for the function supplied to return the pointer passed to it
-- under any condition.
withSyncPtr :: ForeignPtr () -> (Ptr a -> IO b) -> IO b
withSyncPtr fPtr f = withForeignPtr fPtr (f . castPtr) 

-- | The only exported method to access the opaque connection object is wrapped
-- with using the MVar to access it. The map may be altered, but the connection
-- object and its state cannot be modified.
--
-- This will obtain a lock and is not reentry safe.
--
-- Throws 'ClosedConnectionException' if the connection is closed.
withOpaque :: BusyBee o
           => ConnectionWrapper o
           -> (Ptr o -> HandleMap -> IO HandleMap)
           -> IO ()
withOpaque wrapper f = 
  safeWithSynchronized_ wrapper $ \sync@(SynchronizedData {..}) -> do
    newMap <- withSyncPtr ptr_ $ \ptr -> f ptr map_
    return $ sync { map_ = newMap }

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
  safeWithSynchronized_ wrapper $ \(SynchronizedData {..}) -> do 
    -- Fork off a thread to end all pending transactions.
    forkIO $ do
      mapM_ (\(HandleCallback cont) -> cont Nothing) $ HandleMap.elems map_
      -- Runs 'destroy' and then runs 'freePool' on the memory pool due to impl.
      -- of 'connect'
      finalizeForeignPtr ptr_
    -- Put back a nulled, closed, empty connection value.
    nullForeignPtr <- newForeignPtr_ nullPtr
    return $ SynchronizedData nullForeignPtr True HandleMap.empty

-- | Run the event loop once for a length determed by an input timeout.
run :: BusyBee o
    => ConnectionWrapper o
    -> Int32
    -> IO (ReturnCode o)
run wrapper timeout = do
  safeWithSynchronized wrapper $ \sync@(SynchronizedData {..}) -> do
    opts <- getOptions wrapper
    (h, returnCode) <- withSyncPtr ptr_ $ \ptr -> loop opts ptr (CInt timeout)
    newSync <-
      case returnCodeType returnCode of
        ReturnCodeSuccess -> do
          case HandleMap.lookup h map_ of
            Nothing       -> do
              -- Is this an inconsistency error? We just received a success status
              -- with a handle that we haven't stored.
              -- TODO: Investigate this.
              throwIO LoopInconsistency
            Just callback -> do
              asyncRunCallback wrapper callback returnCode
              let newMap = HandleMap.delete h map_
              return $ sync { map_ = newMap }
        ReturnCodeTransientError -> do
          -- Transient statuses include _TIMEOUT and _INTERRUPTED
          return $ sync
        ReturnCodeFatalError -> do
          -- Fatal errors include _POLLFAILED, _ADDFDFAIL, _SHUTDOWN
          throwIO LoopFatalError
        ReturnCodeNonepending -> do
          -- No pending txns? If map_ is not empty then we have a problem.
          case HandleMap.null map_ of
            True  -> return $ sync
            False -> throwIO LoopInconsistency
        _ -> throwIO ReturnCodeNonExhaustive
    return (newSync, returnCode)

-- | Run a 'HandleCallback' continuation in a new thread.
--
-- If the callback completes and returns a new handle and callback, it will be
-- added to the 'HandleMap' of the owning connection.
asyncRunCallback :: BusyBee o
                 => ConnectionWrapper o
                 -> HandleCallback
                 -> ReturnCode o
                 -> IO ()
asyncRunCallback wrapper (HandleCallback entry) returnCode = do
  _ <- forkIO $ do
    cont <- entry $ Just (fromEnum returnCode)
    case cont of
      Nothing -> return ()
      Just (h, e) -> withOpaque wrapper $ const $ \map -> return $ HandleMap.insert h e map
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

