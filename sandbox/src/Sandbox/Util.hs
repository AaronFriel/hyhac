{-# LANGUAGE FlexibleContexts, ScopedTypeVariables #-}

module Sandbox.Util
 where

import Sandbox.SafePrint

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.Base
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import Control.Monad.Trans.Resource.Internal
import Data.IORef
import System.Random

-- PAD OUTPUT

pad :: Int -> Char -> String -> String
pad n c str = replicate (n - length str) c ++ str

-- THREAD DELAYS

threadDelayms :: Int -> IO ()
threadDelayms = threadDelay . (*1000)

randomDelay :: IO ()
randomDelay = do
  delay <- randomRIO (-10, 25)
  when (delay > 0) (threadDelayms delay)

-- QUEUES

newtype InTQueue a = InTQueue (TQueue a)
newtype OutTQueue a = OutTQueue (TQueue a)

newSplitTQueueIO :: IO (InTQueue a, OutTQueue a)
newSplitTQueueIO = fmap splitTQueue newTQueueIO

splitTQueue :: TQueue a -> (InTQueue a, OutTQueue a)
splitTQueue c = (InTQueue c, OutTQueue c)

writeOutTQueue :: OutTQueue a -> a -> STM ()
writeOutTQueue (OutTQueue c) a = writeTQueue c a

writeOutTQueueIO :: MonadIO m => OutTQueue a -> a -> m ()
writeOutTQueueIO (OutTQueue c) a = liftIO $ atomically $ writeTQueue c a

readInTQueue :: InTQueue a -> STM a
readInTQueue (InTQueue c) = readTQueue c

readInTQueueIO :: MonadIO m => InTQueue a -> m a
readInTQueueIO (InTQueue c) = liftIO $ atomically $ readTQueue c

-- CHANNELS

newSplitChan :: IO (InChan a, OutChan a)
newSplitChan = fmap splitChan newChan

splitChan :: Chan a -> (InChan a, OutChan a)
splitChan c = (InChan c, OutChan c)

newtype InChan a = InChan (Chan a)
newtype OutChan a = OutChan (Chan a)

writeOutChan :: OutChan a -> a -> IO ()
writeOutChan (OutChan c) a = writeChan c a

readInChan :: InChan a -> IO a
readInChan (InChan c) = readChan c

-- STREAMS

streamSequence_ :: (a -> IO ()) -> Stream a -> IO ()
streamSequence_ f (Stream r) = do
  value <- r
  case value of
    Just (x, rest) -> f x >> streamSequence_ f rest
    Nothing -> return ()

data Stream a = Stream { unStream :: IO (Maybe (a, Stream a)) }

chanToStream :: Chan (Maybe a) -> IO () -> Stream a
chanToStream chan action = Stream $ do
  action
  item <- readChan chan
  case item of
    Just x -> return $ Just (x, chanToStream chan action)
    Nothing -> return Nothing

-- RESOURCES

loggedResource :: String -> (String -> IO ()) -> Resource String
loggedResource str logger =
  mkResource alloc'
             free'
  where
    alloc' = do
      logger $ "Allocated " ++ str
      return str
    free' str' = do
      logger $ "Freed " ++ str'

loggedIORefResource :: Show a => a -> (String -> IO ()) -> Resource (IORef a)
loggedIORefResource initial logger =
  mkResource alloc' free'
  where
    alloc' = do
      logger $ "Allocated IORef, initial value: " ++ show initial
      newIORef initial
    free' ref = do
      str' <- readIORef ref
      logger $ "Freed IORef, initial value: " ++ show initial
                ++ ", current: " ++ show str'
      -- IORefs are garbage collected

-- | Run a ResourceT in an environment, capturing the resources it allocates and
-- registering them in a parent MonadResource. Return the release key for the 
-- captured resources, and the result of the action.
unwrapResourceT :: (MonadResource m, MonadBase IO m)
                => ResourceT IO a
                -> m (ReleaseKey, a)
unwrapResourceT (ResourceT r) = do
  -- TODO: run this past Gabriel and see if I need to do anything more to mask
  -- exceptions
  istate <- createInternalState
  rkey <- register (stateCleanup istate)
  result <- liftIO $ r istate
  return (rkey, result)

-- | Run a ResourceT in an environment, capturing the resources it allocates and
-- registering them in a parent MonadResource. Return the release key for the 
-- captured resources, and the result of the action.
--
-- This catches any exceptions from the input action, releasing resources and
-- returning 'Nothing' if an exception is caught.
tryUnwrapResourceT :: (MonadResource m, MonadBase IO m)
                   => ResourceT IO a
                   -> m (Maybe (ReleaseKey, a))
tryUnwrapResourceT (ResourceT r) = do
  -- TODO: run this past Gabriel and see if I need to do anything more to mask
  -- exceptions
  istate <- createInternalState
  rkey <- register (stateCleanup istate)
  liftIO $ catch (runAction istate rkey) (handleException rkey) 
  where
    runAction istate rkey = do
      result <- r istate
      return $ Just (rkey, result)
    handleException rkey = \(_ :: SomeException) -> do
      release rkey
      return Nothing

whenMaybe :: Maybe a -> (m b) -> (a -> m b) -> m b
whenMaybe (Just a) _ f = f a
whenMaybe _        n _ = n

