{-# LANGUAGE RecordWildCards, ScopedTypeVariables, BangPatterns, Rank2Types #-}

module Sandbox.Hyhac
  -- ( Client )
  --
  -- , connect
  -- , hyhacDeferred
  -- , hyhacIterator
  -- )
 where

import Sandbox.HyperDexClient
import Sandbox.SafePrint
import Sandbox.Util

import Control.Exception hiding (handle)
import Control.Concurrent
import Data.IORef
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.Resource

import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap

-- | Emulate a client data type.
data Client = Client
  { clientControl :: OutTQueue HyhacCommand
  , clientStatus  :: TMVar ReturnCode
  }

-- | A control channel for executing actions on a data structure.
data HyhacCommand =
    Call (ResIO (CCall, ReturnCode -> IO Bool))
    -- ^An operation to run against the event loop.
  | Demand Handle
    -- ^A result value for a is synchronously demanded.
  | LoopReady
    -- ^The HyperDex event loop has a value ready.
    -- Will be implemented by select/epoll/&c. on a file descriptor.

type HandleMap = HashMap Handle (ReleaseKey, ReturnCode -> IO Bool)

data CallType = DeferredCall | IteratorCall
  deriving (Show, Eq)

type CCall = ClientPtr -> IO Handle

type Callback = ReturnCode -> IO Bool

connect :: IO Client
connect = do
  clientPtr <- try clientCreate
  case clientPtr of 
    Left (e :: SomeException) -> do
      status <- newTMVarIO LoopFailure
      (_,outQ) <- newSplitTQueueIO
      return $ Client outQ status
    Right ptr -> do
      status <- newEmptyTMVarIO
      (inQ, outQ) <- newSplitTQueueIO
      void $ forkIO $ runHyhacLoop status inQ ptr
      clientRegisterCallback (writeOutTQueueIO outQ LoopReady) ptr
      return $ Client outQ status

clientCall :: Client -> HyhacCommand -> IO ()
clientCall (Client {..}) cmd = do
  failureCode <- atomically $ tryReadTMVar clientStatus
  case failureCode of 
    Just rc -> 
      case cmd of 
        Call c -> failCallback rc c
        _      -> return ()
    Nothing -> writeOutTQueueIO clientControl cmd

isTransient :: ReturnCode -> Bool
isTransient rc = rc `elem` [LoopInterrupted]

isGlobal :: ReturnCode -> Bool
isGlobal rc = rc `elem` [LoopFailure]

isNonePending :: ReturnCode -> Bool
isNonePending rc = rc `elem` [LoopNonePending]

allocateHandleMap :: (MonadResource m) 
                  => TMVar ReturnCode
                  -> m (ReleaseKey, IORef HandleMap)
allocateHandleMap status = do
  allocateResource $ mkResource alloc free
  where 
    alloc = newIORef HashMap.empty
    free mapRef = do
      hMap <- readIORef mapRef
      logPrint 9 "handleMap free" 
        $ if HashMap.null hMap
          then "Loop shutdown."
          else "Loop failed, handles open: " ++ show (HashMap.keys hMap)
      maybeRc <- atomically $ tryReadTMVar status
      leftoverMap <- releaseMap (maybe LoopFailure id maybeRc) hMap
      releaseAll leftoverMap

-- | Runs a control loop, synchronously. Should fork before calling.
runHyhacLoop :: TMVar ReturnCode -> InTQueue HyhacCommand -> ClientPtr -> IO ()
runHyhacLoop status queue ptr = runResourceT $ do
  -- On failure, fork a secondary loop to handle all remaining commands.
  _ <- register $ void $ forkIO $ do
    -- okay to throw away exceptions 
    voidException $ hyhacLoopFailure queue ptr LoopFailure
  -- Allocate a handle map reference to be cleaned up on shut down.
  (_,mapRef) <- allocateHandleMap status
  -- And in the event of loop shutdown or exception, set the status to a failure
  -- code.
  _ <- register $ void $ atomically $ tryPutTMVar status LoopFailure
  -- Run the loop and catch any exception that is thrown - that's okay because
  -- it will end the ResourceT and appropriately
  voidException $ runResourceT $ hyhacLoop queue ptr failLoop mapRef
    where
      failLoop rc = liftIO $ do
        void $ atomically $ tryPutTMVar status rc
        throw $ ErrorCall "Loop shutdown"

-- Run an IO computation and catch any exceptions, returning a value in MonadIO.
voidException :: MonadIO m => IO a -> m ()
voidException f = 
  liftIO $ (void $ f)

-- | Loops forever waiting on commands to run against HyperDex.
--
-- If the control channel deadlocks or an exception is uncaught, use of
-- ResourceT ensures that all pending transactions will have their resources
-- freed.
hyhacLoop :: InTQueue HyhacCommand
          -> ClientPtr
          -> (forall a. ReturnCode -> ResIO a)
          -> IORef HandleMap
          -> ResIO ()
hyhacLoop queue ptr failLoop mapRef = do
  inMap <- liftIO $ readIORef mapRef
  cmd <- liftIO $ readInTQueueIO queue
  !outMap <- case cmd of
    Call c -> do
      -- If no exceptions are thrown, return a pair of the releaseKey for the
      -- resources allocated by the call as well as the return value (handle,
      -- callback). Otherwise, 'Nothing'.
      ret <- tryUnwrapResourceT $ do
        (ccall, callback) <- c
        handle <- lift $ ccall ptr
        return (handle, callback)
      case ret of
        Just (state,(handle,callback)) -> do
          case HashMap.lookup handle inMap of
            -- if a duplicate handle is received, clean up state previously
            -- used by that handle.
            Just (priorState, _) -> release priorState
            _                    -> return ()
          return $ HashMap.insert handle (state, callback) inMap
        _ -> return inMap
    Demand handle -> do
    --  logPrint 9 "hyhacLoop" $ "Received demand" 
      return inMap
    --  handleResult inMap $ loopUntil handle ptr
    LoopReady -> do
      logPrint 9 "hyhacLoop" $ "Received loopReady" 
      handleResult inMap $ loopOnce ptr
  lift $ writeIORef mapRef outMap 
  hyhacLoop queue ptr failLoop mapRef
  where
    handleResult !inMap !f = do
      (result, !outMap) <- lift $ f inMap
      case result of
        Left rc
          | isNonePending rc -> do
              logPrint 9 "hyhacLoop - handleResult" $ "NonePending code: " ++ show rc
              emptyMap <- releaseMap LoopNonePending inMap
              return emptyMap
          | isGlobal rc    -> do
              logPrint 9 "hyhacLoop - handleResult" $ "Global code: " ++ show rc
              failLoop rc
          | isTransient rc -> do
              logPrint 9 "hyhacLoop - handleResult" $ "Transient code: " ++ show rc
              handleResult outMap f
          | otherwise  -> do
              logPrint 9 "hyhacLoop - handleResult" $ "Other code: " ++ show rc
              return outMap
        Right _ -> return outMap

hyhacLoopFailure :: InTQueue HyhacCommand
                 -> ClientPtr
                 -> ReturnCode
                 -> IO a
hyhacLoopFailure queue ptr rc = do
  cmd <- readInTQueueIO queue
  case cmd of
    Call c -> do
      liftIO $ failCallback rc c
    _ -> return ()
  hyhacLoopFailure queue ptr rc

loopOnce :: ClientPtr -> HandleMap -> IO (Either ReturnCode Handle, HandleMap)
loopOnce ptr !inMap = do
  (h,rc) <- clientLoop ptr
  case () of
    _ | isGlobal rc    -> return (Left rc, inMap)
      | h >= 0         ->
          case HashMap.lookup h inMap of
            Just (s, c) -> do
              !outMap <- runCallback rc h (s, c) inMap
              return (Right h, outMap)
            Nothing    -> return (Right h, inMap)
      | otherwise      -> return (Left rc, inMap)

loopUntil :: Handle -> ClientPtr -> HandleMap
          -> IO (Either ReturnCode Handle, HandleMap)
loopUntil testHandle ptr inMap = do
  (ret, outMap) <- loopOnce ptr inMap
  case ret of
    Left rc
      | isTransient rc -> yield >> loopUntil testHandle ptr outMap
      | otherwise      -> return $ (Left rc, outMap)
    Right h
      | h == testHandle -> return $ (Right h, outMap)
      | otherwise       -> yield >> loopUntil testHandle ptr outMap

failCallback :: ReturnCode -> ResIO (CCall, ReturnCode -> IO Bool) -> IO ()
failCallback rc c = do
  logPrint 9 "failCallback" $ "Received call during failure"
  void $ forkIO $ void $ runResourceT $ tryUnwrapResourceT $ do
    (_, callback) <- c
    void $ lift $ callback rc

runCallback :: MonadIO m => ReturnCode -> Handle -> (ReleaseKey, Callback)
            -> HandleMap -> m (HandleMap)
runCallback rc h (state, callback) inMap = do
  done <- liftIO $ callback rc
  case done of
    True -> do
      release state
      return $ HashMap.delete h inMap
    False -> return inMap

releaseMap :: MonadIO m => ReturnCode -> HandleMap -> m HandleMap
releaseMap rc hMap = do
  leftoverMap <- foldM fold hMap (HashMap.toList hMap)
  releaseAll leftoverMap
  return HashMap.empty
  where 
    fold priorMap (h,(state,callback)) =
      runCallback rc h (state, callback) priorMap

releaseAll :: MonadIO m => HandleMap -> m ()
releaseAll hMap = do
  forM_ (HashMap.elems hMap) $ \(state, _) -> do
    release state

captureHandle :: MVar Handle -> (a -> IO Handle) -> (a -> IO Handle)
captureHandle mvar f = \a -> do
  h <- f a
  putMVar mvar h
  return h

data CallDescription t r a b = CallDescription
  { callType           :: CallType
  , resultIntermediate :: IO t
  , failureAction      :: t -> r -> IO ()
  , defaultFailureCode :: ReturnCode
  , successAction      :: t -> a -> IO ()
  , completeAction     :: t -> IO ()
  , returnCodeComplete :: ReturnCode -> Bool
  , returnCodeSuccess  :: ReturnCode -> Bool
  , returnResult       :: Client -> MVar Handle -> t -> b
  }

wrapDeferred:: ResIO (CCall, ResIO a) -> Client -> IO (IO (Either ReturnCode a))
wrapDeferred = wrapGeneral deferred
  where
    deferred = CallDescription
      { callType = DeferredCall
      , resultIntermediate = newEmptyMVar
      , failureAction = \m rc -> do
          logPrint 6 "wrapDeferred" $ "Failure action"
          void $ tryPutMVar m $ Left rc
      , defaultFailureCode = LoopFailure
      , successAction = \m a -> putMVar m $ Right a
      , completeAction = const $ return ()
      , returnCodeComplete = (== LoopSuccess)
      , returnCodeSuccess  = (== LoopSuccess)
      , returnResult = \client hvar m -> 
                          mvarToAsync m (demandHandle hvar client)
      }

wrapIterator :: ResIO (CCall, ResIO a) -> Client -> IO (Stream (Either ReturnCode a))
wrapIterator = wrapGeneral iterator
  where
    iterator = CallDescription
      { callType = IteratorCall
      , resultIntermediate = newTQueueIO
      , failureAction = \m rc -> do
          logPrint 6 "wrapIterator" $ "Failure action"
          atomically $ do 
            writeTQueue m $ Just (Left rc)
            writeTQueue m $ Nothing
      , defaultFailureCode = LoopFailure
      , successAction = \m a -> atomically $ writeTQueue m $ Just (Right a)
      , completeAction = \m -> atomically $ writeTQueue m $ Nothing
      , returnCodeComplete = (== LoopSearchDone)
      , returnCodeSuccess  = (== LoopSuccess)
      , returnResult = \client hvar m ->
                          queueToStream m (demandHandle hvar client)
      }

wrapGeneral :: CallDescription t ReturnCode a b
            -> ResIO (CCall, ResIO a)
            -> Client
            -> IO b
wrapGeneral (CallDescription {..}) = \hyhacCall client -> do
  output <- resultIntermediate
  handleVar <- newEmptyMVar
  clientCall client $ Call $ do
    -- In the event that this call does not complete, call the failure action
    -- and set the handle to an invalid result.
    rkey <- register $ do
      logPrint 3 "wrapGeneral" $ "Registered failure action fired"
      void $ tryPutMVar handleVar (-1)
      failureAction output defaultFailureCode
    -- Allocating the resources necessary for the Hyperdex call, register them
    -- and return the C function call and callback. No attempt is made to
    -- bracket the call. If an exception is thrown the action registered above
    -- will ensure failure modes are correctly handled.
    (ccall, callback) <- hyhacCall
    --
    let -- Routine to prevent failure action from firing
        unregister = void $ unprotect rkey
        -- Rewrite the C call to take the handle generated, put it in HandleVar
        ccall' = captureHandle handleVar ccall
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
                  logPrint 3 "wrapGeneral" $ "Callback - failure: " ++ show rc
                  unregister
                  void $ tryPutMVar handleVar (-1)
                  failureAction output rc
          return complete
    return (ccall', callback')
  return $ returnResult client handleVar output

-- | Send a signal to the client that a handle is demanded synchronously.
demandHandle :: MVar Handle -> Client -> IO ()
demandHandle handleVar client = do
  handle <- readMVar handleVar
  clientCall client $ Demand handle

-- | Transform an MVar that will be filled by a callback into an async value.
mvarToAsync :: MVar a -> IO () -> IO a
mvarToAsync m demand = do
  result <- tryTakeMVar m
  case result of
    Just a -> return a
    Nothing -> do
      demand
      yield
      takeMVar m

queueToStream :: TQueue (Maybe a) -> IO () -> Stream a
queueToStream queue demand = Stream $ do
  maybeItem <- atomically $ tryReadTQueue queue
  case maybeItem of
    Just a  -> stream a
    Nothing -> do
      demand
      yield
      a <- atomically $ readTQueue queue
      stream a
  where
    stream (Just a) = return $ Just (a, queueToStream queue demand)
    stream Nothing  = return Nothing

hyhacDeferred :: Client -> IO (IO (Either ReturnCode String))
hyhacDeferred = wrapDeferred $ do
  logPrint 5 "hyhacDeferred" "Entered hyhacDeferred"
  -- Simulate allocating some resources for the call
  void $ allocateResource $ loggedResource "a" logger
  void $ allocateResource $ loggedResource "b" logger
  void $ allocateResource $ loggedResource "c" logger
  -- allocate an IORef (simulated pointer) to some data that will be set by the
  -- foreign function call
  (_,ref) <- allocateResource $ loggedIORefResource "deferredGarbage" logger
  -- define the foreign function call and callback functions
  let call ptr = clientDeferredCall ptr ref
  let callback = do
        logPrint 6 "hyhacDeferred - Callback" $ "Reading IORef"
        void $ allocateResource $ loggedResource "someStruct" logger
        liftIO $ readIORef ref
  return (call, callback)
  where 
    logger = logPrint 7 "hyhacDeferred - Resource"

hyhacIterator :: Client -> IO (Stream (Either ReturnCode String))
hyhacIterator = wrapIterator $ do
  logPrint 5 "hyhacIterator" "Entered hyhacIterator"
  -- Simulate allocating some resources for the call
  void $ allocateResource $ loggedResource "x" logger
  void $ allocateResource $ loggedResource "y" logger
  void $ allocateResource $ loggedResource "z" logger
  -- allocate an IORef (simulated pointer) to some data that will be set by the
  -- foreign function call
  (_,ref) <- allocateResource $ loggedIORefResource "iteratorGarbage" logger
  -- define the foreign function call and callback functions
  let call ptr = clientIteratorCall ptr ref
  let callback = do
        logPrint 6 "hyhacIterator - Callback" $ "Reading IORef"
        void $ allocateResource $ loggedResource "someStruct" logger
        liftIO $ readIORef ref
  return (call, callback)
  where
    logger = logPrint 7 "hyhacIterator - Resource"