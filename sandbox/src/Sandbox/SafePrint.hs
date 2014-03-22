module Sandbox.SafePrint
     ( sPrint
     , logPrint
     , syncLogPrint
     , setVerbosity
     , setSynchronousLogging
     )
 where

import Control.Concurrent
import Control.Monad
import Control.Monad.IO.Class
import System.IO.Unsafe
import Data.IORef

printLock :: MVar ()
printLock = unsafePerformIO (newMVar ())
{-# NOINLINE printLock #-}

logVerbosity :: IORef Int
logVerbosity = unsafePerformIO (newIORef 5)
{-# NOINLINE logVerbosity #-}

logSync :: IORef Bool
logSync = unsafePerformIO (newIORef False)
{-# NOINLINE logSync #-}

forkSync :: IO () -> IO ()
forkSync f = do
	sync <- readIORef logSync
	case sync of
		True  -> f
		False -> void . forkIO $ f

setSynchronousLogging :: Bool -> IO ()
setSynchronousLogging = writeIORef logSync

setVerbosity :: Int -> IO ()
setVerbosity = writeIORef logVerbosity

sPrint :: MonadIO m => String -> m ()
sPrint x = liftIO $ logPrint 5 "sPrint" x
{-# NOINLINE sPrint #-}

logPrint :: MonadIO m => Int -> String -> String -> m ()
logPrint level src str = liftIO $ do
	verbosity <- readIORef logVerbosity
	let msg = replicate level ' ' ++ src ++ ": " ++ str
	when (level <= verbosity) $ do
		forkSync $ withMVar printLock $ (const $ putStrLn msg)
{-# NOINLINE logPrint #-}

syncLogPrint :: MonadIO m => Int -> String -> String -> m ()
syncLogPrint level src str = liftIO $ do
	verbosity <- readIORef logVerbosity
	let msg = replicate level ' ' ++ src ++ ": " ++ str
	when (level <= verbosity) $ do
		withMVar printLock $ (const $ putStrLn msg)
{-# NOINLINE syncLogPrint #-}
