{-# LANGUAGE ExistentialQuantification, TypeFamilies, BangPatterns, RecordWildCards #-}
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
  ( HyperDex (..)
  , HyperDexConnection
  , HyperDexResult
  , AsyncResult
  , Stream
  , readStream
  , AsyncCall (..)
  , SyncCall (..)
  , connect
  , wrapDeferred
  , wrapIterator
  , wrapImmediate
  )
  where

import Database.HyperDex.Internal.Handle (Handle, handleSuccess, invalidHandle)
import qualified Database.HyperDex.Internal.Handle as HandleMap
import Database.HyperDex.Internal.Options
import Database.HyperDex.Internal.Util

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import Data.IORef
import Foreign.C

-- | A generalization over the two types of HyperDex connections, admin and
-- client.
--
-- The terminology here may not be precisely correct - but this is effectively
-- an interface to the HyperDex event loop that is wrapped around in the HyperDex
-- client library.
class HyperDex o where
  data ReturnCode o

  failureCode :: ReturnCode o
  isTransient :: ReturnCode o -> Bool
  isGlobalError :: ReturnCode o -> Bool
  isNonePending :: ReturnCode o -> Bool

  deferredSuccess :: ReturnCode o -> Bool
  iteratorSuccess :: ReturnCode o -> Bool
  iteratorComplete :: ReturnCode o -> Bool

  -- | Create a connection.
  create :: CString
         -- ^ The hostname or IP address of the coordinator.
         -> CUShort
         -- ^ The port number.
         -> IO o
         -- ^ Returns pointer to opaque structure.

  -- | Destroy the connection.
  destroy :: o
          -- ^ Pointer to opaque structure
          -> IO ()

  -- | Poll the event loop for 'Int' milliseconds.
  loop :: o
       -- ^ A pointer to opaque structure
       -> CInt
       -- ^ The time to delay, typically in microseconds.
       -> IO (Handle, ReturnCode o)

-- | Wrapper around connections carrying the information for the connection
-- along with the control channel and current status.
data HyperDex o => HyperDexConnection o = HyperDexConnection
  { control_ :: {-# UNPACK #-} !(ControlWriter o)
  , status_  :: {-# UNPACK #-} !(ConnectionStatus o)
  , info_    :: {-# UNPACK #-} !ConnectInfo
  }

data Command o = RunAsync  {-# UNPACK #-} !(Wrapped o)
               | RunSync                  !(o -> IO ())
               | Demand    {-# UNPACK #-} !Handle
             --  | LoopReady {-# UNPACK #-} !Handle

-- | Control mechanism to run commands against HyperDex client
type ControlWriter o = OutTQueue (Command o)
type ControlReader o = InTQueue (Command o)

-- | Current status of HyperDex client connection (healthy if empty)
type ConnectionStatus o = TMVar (ReturnCode o)

type Callback o = ReturnCode o -> IO Bool

data Wrapped o = Wrapped {-# UNPACK  #-} !ReleaseKey
                                         !(o -> IO Handle)
                                         !(Callback o)

data AsyncCall o a = AsyncCall !(o -> IO Handle)
                               !(ResIO (HyperDexResult o a))

data SyncCall o a = forall r. SyncCall !(o -> IO r)
                                       !(r -> ResIO (HyperDexResult o a))

type HandleMap o = HandleMap.HandleMap (ReleaseKey, Callback o)

-- | 'HyperDexResult' represents a single HyperDex return value.
type HyperDexResult o a = Either (ReturnCode o) a

-- | 'AsyncResult' represents an action synchronously demanding the result
-- of a call that produces a single value.
type AsyncResult o a = IO (HyperDexResult o a)

-- | 'Stream' represents an asynchronous stream of results produced by a call
-- against HyperDex. Each invocation of 'readStream' produces a value or
-- 'Nothing', in which case all subsequent results return 'Nothing'.
--
-- The first argument is a function that issues a 'demand' for a value
-- synchronously. That is, it informs the client that there is a blocking wait
-- on new values.
data Stream o a = Stream !(TQueue (Maybe (HyperDexResult o a)))
                         !(IO ())

makeStream :: TQueue (Maybe (HyperDexResult o a)) -> IO () -> Stream o a
makeStream = Stream

-- | Try to read a value from a queue, if 'Just Nothing', place it back on top.
tryReadStreamUndo :: TQueue (Maybe a) -> STM (Maybe (Maybe a))
tryReadStreamUndo q = do
  a <- tryReadTQueue q
  case a of
    Just Nothing -> do
      unGetTQueue q Nothing
      return a
    _            -> return a

readStreamUndo :: TQueue (Maybe a) -> STM (Maybe a)
readStreamUndo q = do
  a <- readTQueue q
  case a of
    Nothing -> do
      unGetTQueue q a
      return a
    _       -> return a

-- | Try to read a value from a stream. If the stream is closed, it will return
-- 'Nothing', and return 'Nothing' for all subsequent calls.
readStream :: Stream o a -> IO (Maybe (HyperDexResult o a))
readStream (Stream q d) = do
  maybeItem <- atomically $ tryReadStreamUndo q
  case maybeItem of
    Just a -> return a
    Nothing -> do
      d
      atomically $ readStreamUndo q

-- | Connect to HyperDex and set up control channel to run commands against the
-- client.
connect :: (HyperDex o)
        => ConnectInfo
        -> IO (HyperDexConnection o)
connect info = runResourceT $ do
  let port = CUShort $ connectPort info
  host <- rNewCBString0 $ connectHost info
  liftIO $ do
    ptr <- create host port
    status <- newEmptyTMVarIO
    (inQ, outQ) <- newSplitTQueueIO
    void $ forkIO $ runHyhacLoop status inQ ptr
    -- TODO:
    -- register listener for HyperDex loop fd here
    return $ HyperDexConnection outQ status info

clientCall :: HyperDex o
           => HyperDexConnection o
           -> Command o
           -> IO ()
clientCall (HyperDexConnection {..}) cmd = do
  -- let (HyperDexConnection {..}) = getConnection conn
  cleanup <- atomically $ do
    status <- tryReadTMVar status_
    case status of
      Just rc ->
        case cmd of
          RunAsync c -> return (failCallback rc c)
          _         -> return (return ())
      Nothing -> do
        writeOutTQueue control_ cmd
        return (return ())
  cleanup

-- | Send a signal to the client that a handle is demanded synchronously.
demandHandle :: HyperDex o => MVar Handle -> HyperDexConnection o -> IO ()
demandHandle handleVar client = do
  handle <- readMVar handleVar
  clientCall client $ Demand handle

-- | Loop that operates on control channel and executes command. Runs
-- synchronously. Should fork before calling.
--
-- This loop will read commands from its queue and execute them against the
-- opaque client pointer. This is done to ensure synchronized access to the
-- pointer without managing locks. Consequently, only one loop should execute
-- at a time.
--
-- This loop is expected to eventually fail with an 'BlockedIndefinitelyOnSTM'
-- exception. This is normal, and will occur if the garbage collector finds no
-- live references to the control channel.
--
-- Upon termination resources are registered that will set the status of the
-- loop to a failure state (if it has not already), clean up all pending
-- callbacks, and launch a failure loop that will respond to all incoming
-- commands with a failure condition.
runHyhacLoop :: (HyperDex o)
             => ConnectionStatus o
             -> ControlReader o
             -> o
             -> IO ()
runHyhacLoop status queue ptr = runResourceT $ do
  void $ register $ forkIO_ $ do
    void $ atomically $ tryPutTMVar status failureCode
    hyhacLoopFailure queue failureCode
  (_,mapRef) <- allocateHandleMap status
  hyhacLoop queue ptr loopFail mapRef
  where
    loopFail rc = liftIO $ do
      void $ atomically $ tryPutTMVar status rc
      throw $ ErrorCall "Loop shutdown"

-- | Loops forever waiting on commands to run against HyperDex.
--
-- If the control channel deadlocks or an exception is uncaught, use of
-- ResourceT ensures that all pending transactions will have their resources
-- freed.
-- hyhacLoop :: InTQueue Command
--           -> ClientPtr
--           -> (forall a. ReturnCode -> ResIO a)
--           -> IORef HandleMap
--           -> ResIO ()
hyhacLoop queue ptr failLoop mapRef = forever $ do
  inMap <- liftIO $ readIORef mapRef
  cmd <- liftIO $ readInTQueueIO queue
  !outMap <- case cmd of
    RunAsync (Wrapped state ccall callback) -> do
      handle <- liftIO $ ccall ptr
      case HandleMap.lookup handle inMap of
        -- if a duplicate handle is received, clean up state previously
        -- used by that handle.
        Just (priorState, _) -> release priorState
        _                    -> return ()
      return $ HandleMap.insert handle (state, callback) inMap
    RunSync f -> do
      liftIO $ f ptr
      return inMap
    Demand handle -> do
      handleResult inMap $ loopUntil handle ptr
    -- LoopReady -> do
    --   handleResult inMap $ loopOnce ptr
  liftIO $ writeIORef mapRef outMap
  where
--    handleResult :: HandleMap -> IO
    handleResult !inMap !f = do
      (result, !outMap) <- liftIO $ f inMap
      case result of
        Left rc
          | isNonePending rc -> do
              emptyMap <- releaseMap failureCode inMap
              return emptyMap
          | isGlobalError rc    -> do
              failLoop rc
          | isTransient rc -> do
              handleResult outMap f
          | otherwise  -> do
              return outMap
        Right _ -> return outMap

loopOnce :: HyperDex o
         => o
         -> HandleMap o
         -> IO (Either (ReturnCode o) Handle, HandleMap o)
loopOnce ptr !inMap = do
  (h,rc) <- loop ptr 0
  case () of
    _ | isGlobalError rc -> return (Left rc, inMap)
      | handleSuccess h  ->
          case HandleMap.lookup h inMap of
            Just (s, c) -> do
              !outMap <- runCallback rc h (s, c) inMap
              return (Right h, outMap)
            Nothing    -> return (Right h, inMap)
      | otherwise      -> return (Left rc, inMap)

loopUntil :: HyperDex o
          => Handle
          -> o
          -> HandleMap o
          -> IO (Either (ReturnCode o) Handle, HandleMap o)
loopUntil testHandle ptr inMap = do
  (ret, outMap) <- loopOnce ptr inMap
  case ret of
    Left rc
      | isTransient rc -> yield >> loopUntil testHandle ptr outMap
      | otherwise      -> return $ (Left rc, outMap)
    Right h
      | h == testHandle -> return $ (Right h, outMap)
      | otherwise       -> yield >> loopUntil testHandle ptr outMap

hyhacLoopFailure :: HyperDex o
                 => ControlReader o
                 -> ReturnCode o
                 -> IO ()
hyhacLoopFailure queue rc = forever $ do
  cmd <- readInTQueueIO queue
  case cmd of
    RunAsync c -> do
      liftIO $ failCallback rc c
    _ -> return ()

failCallback :: HyperDex o
             => ReturnCode o
             -> Wrapped o
             -> IO ()
failCallback rc (Wrapped state _ callback) = do
  forkIO_ $ do
    callback rc
    release state

allocateHandleMap :: (HyperDex o, MonadResource m)
                  => ConnectionStatus o
                  -> m (ReleaseKey, IORef (HandleMap o))
allocateHandleMap status = do
  allocateResource $ mkResource alloc free
  where
    alloc = newIORef HandleMap.empty
    free mapRef = do
      hMap <- readIORef mapRef
      maybeRc <- atomically $ tryReadTMVar status
      leftoverMap <- releaseMap (maybe failureCode id maybeRc) hMap
      releaseAll leftoverMap

runCallback :: (HyperDex o, MonadIO m)
            => ReturnCode o -> Handle -> (ReleaseKey, Callback o)
            -> HandleMap o -> m (HandleMap o)
runCallback rc h (state, callback) inMap = do
  done <- liftIO $ callback rc
  case done of
    True -> do
      release state
      return $ HandleMap.delete h inMap
    False -> return inMap

releaseMap :: (HyperDex o, MonadIO m)
           => ReturnCode o -> HandleMap o -> m (HandleMap o)
releaseMap rc hMap = do
  leftoverMap <- foldM fold hMap (HandleMap.toList hMap)
  releaseAll leftoverMap
  return HandleMap.empty
  where
    fold priorMap (h,(state,callback)) =
      runCallback rc h (state, callback) priorMap

releaseAll :: (HyperDex o, MonadIO m)
           => HandleMap o -> m ()
releaseAll hMap = do
  forM_ (HandleMap.elems hMap) $ \(state, _) -> do
    release state

data CallDescription o a b = forall t. CallDescription
  { resultIntermediate :: IO t
  , failureAction      :: t -> ReturnCode o -> IO ()
  , defaultFailureCode :: ReturnCode o
  , successAction      :: t -> HyperDexResult o a -> IO ()
  , completeAction     :: t -> IO ()
  , returnCodeComplete :: ReturnCode o -> Bool
  , returnCodeSuccess  :: ReturnCode o -> Bool
  , returnResult       :: HyperDexConnection o -> MVar Handle -> t -> b
  }

wrapDeferred :: HyperDex o
             => (ReturnCode o -> Bool)
             -> ResIO (AsyncCall o a)
             -> HyperDexConnection o
             -> IO (AsyncResult o a)
wrapDeferred deferredSuccess = wrapGeneral deferred
  where
    deferred = CallDescription
      { resultIntermediate = newEmptyMVar :: IO (MVar (HyperDexResult o a))
      , failureAction = \m rc -> do
          void $ tryPutMVar m $ Left rc
      , defaultFailureCode = failureCode
      , successAction = \m a -> putMVar m a
      , completeAction = const $ return ()
      , returnCodeComplete = deferredSuccess
      , returnCodeSuccess  = deferredSuccess
      , returnResult = \client hvar m ->
                          mvarToAsync m (demandHandle hvar client)
      }

wrapIterator :: HyperDex o
             => (ReturnCode o -> Bool)
             -> (ReturnCode o -> Bool)
             -> ResIO (AsyncCall o a)
             -> HyperDexConnection o
             -> IO (Stream o a)
wrapIterator iteratorComplete iteratorSuccess = wrapGeneral iterator
  where
    iterator = CallDescription
      { resultIntermediate = newTQueueIO :: IO (TQueue (Maybe (HyperDexResult o a)))
      , failureAction = \m rc -> do
          atomically $ do
            writeTQueue m $ Just $ Left rc
            writeTQueue m $ Nothing
      , defaultFailureCode = failureCode
      , successAction = \m a -> atomically $ writeTQueue m $ Just a
      , completeAction = \m -> atomically $ writeTQueue m $ Nothing
      , returnCodeComplete = iteratorComplete
      , returnCodeSuccess  = iteratorSuccess
      , returnResult = \client hvar m ->
                          makeStream m (demandHandle hvar client)
      }

wrapGeneral :: HyperDex o
            => CallDescription o a b
            -> ResIO (AsyncCall o a)
            -> HyperDexConnection o
            -> IO b
wrapGeneral (CallDescription {..}) = \hyhacCall client -> do
  output <- resultIntermediate
  handleVar <- newEmptyMVar
  wrappedCall <- runResourceT $ do
    -- In the event that this call does not complete, call the failure action
    -- and set the handle to an invalid result.
    handleKey <- register $ do
      void $ tryPutMVar handleVar invalidHandle
      failureAction output defaultFailureCode

    -- Invert control of the resources of the ResourceT.
    (rkey, AsyncCall ccall callback) <- unwrapResourceT hyhacCall

    let -- Routine to prevent failure action from firing
        unregister = void $ unprotect handleKey
        -- Rewrite the C call to take the handle generated, put it in HandleVar
        ccall' = captureOutput handleVar ccall
        -- Rewrite the callback to handle success/failure modes. There is a 2x2
        -- matrix of return code membership in 'returnCodeSuccess'
        -- 'returnCodeComplete', 'successCodes' and this handles those
        -- possibilities.
        callback' rc = do
          let complete = returnCodeComplete rc
              success = returnCodeSuccess rc
          case (complete, success) of
            (True, True) -> do
                  unregister
                  runResourceT callback >>= successAction output
                  completeAction output
            (True, False) -> do
                  unregister
                  completeAction output
            (False, True) -> do
                  runResourceT callback >>= successAction output
            (False, False) -> do
                  unregister
                  void $ tryPutMVar handleVar invalidHandle
                  failureAction output rc
          return complete
    return $ Wrapped rkey ccall' callback'
  clientCall client $ RunAsync wrappedCall
  return $ returnResult client handleVar output

wrapImmediate :: HyperDex o
              => ResIO (SyncCall o a)
              -> HyperDexConnection o
              -> IO (AsyncResult o a)
wrapImmediate = \hyhacCall client -> do
  bVar <- newEmptyMVar
  clientCall client $ RunSync $ \ptr -> runResourceT $ do
    SyncCall ccall callback <- hyhacCall
    result <- (liftIO $ ccall ptr) >>= callback
    void $ liftIO $ tryPutMVar bVar result
  return $ readMVar bVar

captureOutput :: MVar b -> (a -> IO b) -> (a -> IO b)
captureOutput mvar f = \a -> do
  h <- f a
  void $ tryPutMVar mvar h
  return h

-- | Transform an MVar that will be filled by a callback into an async value.
mvarToAsync :: MVar a -> IO () -> IO a
mvarToAsync m demand = do
  result <- tryReadMVar m
  case result of
    Just a -> return a
    Nothing -> do
      demand
      yield
      readMVar m
