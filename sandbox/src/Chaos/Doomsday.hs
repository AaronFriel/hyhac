{-# LANGUAGE RankNTypes #-}

module Chaos.Doomsday
     ( delayDoomsday
     , stopDoomsday
     , setDoomsdayClock
     , setDoomsdayClockRepeat
     , readDoomsdayClock
     , isitMidnight
     , withDoomsday
     , suspendDoomsday
     , withDoomsdaySTM
     , mkE
     )
 where

import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Control.Concurrent (threadDelay, forkIO)
import Control.Concurrent.STM (STM)
import Data.IORef
import GHC.Conc (unsafeIOToSTM)
import System.IO.Unsafe (unsafePerformIO)
import System.Random (randomRIO)

doomsdayClock :: IORef (Maybe Int)
doomsdayClock = unsafePerformIO (newIORef Nothing)
{-# NOINLINE doomsdayClock #-}

doomsdayRepeat :: IORef (Maybe Int)
doomsdayRepeat = unsafePerformIO (newIORef Nothing)
{-# NOINLINE doomsdayRepeat #-}

-- | Set the doomsday clock to zero in a given number of microseconds.
delayDoomsday :: MonadIO m => Int -> m ()
delayDoomsday delay = liftIO $ void $ forkIO $ do
  threadDelay delay
  writeIORef doomsdayClock $ Just 0

stopDoomsday :: MonadIO m => m ()
stopDoomsday = liftIO $ do
  writeIORef doomsdayClock Nothing

setDoomsdayClockMaybe :: MonadIO m => Maybe Int -> m ()
setDoomsdayClockMaybe time = liftIO $ do
  writeIORef doomsdayClock $ time

setDoomsdayClock :: MonadIO m => Int -> m ()
setDoomsdayClock time = liftIO $ do
  writeIORef doomsdayClock $ Just time

setDoomsdayClockRepeat :: MonadIO m => Maybe Int -> m ()
setDoomsdayClockRepeat cycle = liftIO $ do
  writeIORef doomsdayRepeat cycle

readDoomsdayClock :: MonadIO m => m (Maybe Int)
readDoomsdayClock = liftIO $ do
  readIORef doomsdayClock

isitMidnight :: MonadIO m => m Bool
isitMidnight = liftIO $ do
  time <- readIORef doomsdayClock
  return $ checkMidnight time
  case time of
    Just n | n <= 0 -> return True
    _               -> return False

checkMidnight :: Maybe Int -> Bool
checkMidnight (Just n) | n <= 0 = True
checkMidnight _                 = False

moveClock :: MonadIO m => m Bool
moveClock = liftIO $ do
  midnight <- atomicModifyIORef doomsdayClock $ \a ->
                if checkMidnight a
                then (a,   True)
                else (fmap (subtract 1) a, False)
  repeat <- readIORef doomsdayRepeat
  when (midnight) $ 
    setDoomsdayClockMaybe repeat
  return midnight

throwSomething :: (MonadIO m, Exception e) => [e] -> m a
throwSomething e | null e        = error "Empty exception list."
                 | null (tail e) = throw $ head e
                 | otherwise     = do
                     n <- liftIO $ randomRIO (0, numExceptions)
                     throw $ e !! n
  where numExceptions = length e

whenMidnight :: (MonadIO m)
             => m ()
             -> m ()
whenMidnight f = do
  midnight <- isitMidnight
  when midnight f

withDoomsday :: (MonadIO m, Exception e)
             => [e] -> m a -> m a
withDoomsday e f = do
  midnight <- moveClock
  if midnight
  then throwSomething e
  else f

suspendDoomsday :: MonadIO m => m a -> m a
suspendDoomsday f = do
  time <- readDoomsdayClock
  stopDoomsday 
  result <- f
  setDoomsdayClockMaybe time
  return $ result

withDoomsdaySTM :: (Exception e)
                => [e]
                -> STM a
                -> STM a
withDoomsdaySTM e f = do
  unsafeIOToSTM $ withDoomsday e (return ())
  f

mkE :: (Exception e) => e -> SomeException
mkE = toException
