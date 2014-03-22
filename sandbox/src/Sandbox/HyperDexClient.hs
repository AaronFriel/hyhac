{-# LANGUAGE RecordWildCards #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Sandbox.HyperDexClient
-- Copyright   :  (c) Aaron Friel
-- License     :  BSD3
-- Maintainer  :  mayreply@aaronfriel.com
-- Stability   :  experimental
-- Portability :  non-portable
--
-- An emulator for a HyperDex /Client/. Not the coordinator or daemon component,
-- but the client which is the target of bindings libraries.
--
-- This very simple emulator maintains a list of handles in use, and a queue of
-- handles along with whether or not they are complete, a return code to return,
-- and an action to perform before returning the handle.
--
-- A loop state is maintained, and occasionally emits failure codes.
--
-- Finally, a virtual file descriptor represents a list of callbacks to rur when
-- a value becomes available.
-----------------------------------------------------------------------------

module Sandbox.HyperDexClient
     ( ClientPtr
     , Handle
     , ReturnCode (..)
     , clientCreate
     --, clientDestroy
     , clientLoop
     , clientRegisterCallback
     , clientDeferredCall
     , clientIteratorCall
     )
 where
	
import Sandbox.SafePrint
import Sandbox.Util

import Control.Concurrent (forkIO)
import Chaos.Concurrent.STM
import Control.Monad
import Chaos.IORef
import Data.List
import System.Random

default (Int)

-- | A simulated opaque client pointer.
data ClientPtr = ClientPtr
  { handleQueue :: TQueue (Handle, Bool, ReturnCode, IO ())
  , handleList :: TVar [Handle]
  , loopState :: TVar (LoopState ReturnCode)
  , clientFD :: TVar [IO ()]
  }

-- | A handle uniquely identifying a pending operation. 
type Handle = Int

-- | A return code to pass to the client.
data ReturnCode = LoopSuccess
                | LoopSearchDone
                | LoopInterrupted
                | LoopFailure
                | LoopTimeout
                | LoopNonePending
                | LoopCatastrophic
  deriving (Show, Eq)

-- | An indicator of the current health of the loop. A transient failure is
-- corrected after one iteration. A permanent failure is returned for all
-- subsequent calls.
data LoopState rc = StateHealthy | StateTransient rc | StatePermanent rc

clientCreate :: IO ClientPtr
clientCreate = atomically $ do
  queue <- newTQueue
  handles <- newTVar []
  state <- newTVar StateHealthy
  fd <- newTVar []
  return $ ClientPtr queue handles state fd

clientRegisterCallback :: IO () -> ClientPtr -> IO ()
clientRegisterCallback c (ClientPtr {..}) = do
  atomically $ modifyTVar clientFD (c:)

allocHandle :: ClientPtr -> IO Handle
allocHandle (ClientPtr {..}) = atomically $ do
  handles <- readTVar handleList
  let h = head
        $ dropWhile (\n -> n `elem` handles) [0..]
  writeTVar handleList $ h : handles
  return h

handleReady :: Handle -> Bool -> ReturnCode -> IO () -> ClientPtr -> IO ()
handleReady h d s f (ClientPtr {..}) = do
  callbacks <- atomically $ do
    writeTQueue handleQueue (h, d, s, f)
    readTVar clientFD
  sequence_ callbacks

randomFailure :: TVar (LoopState ReturnCode) -> IO ()
randomFailure state = do
  r <- randomRIO (0.0,1.0) :: IO Double
  atomically $ modifyTVar state (go r)
  where 
    go r 
      | r <  1 / 100000 = const $ StatePermanent LoopFailure
      | r <  1 /   1000 = const $ StateTransient LoopTimeout
      | r <  1 /    500 = const $ StateTransient LoopNonePending
      | r <  1 /     50 = const $ StateTransient LoopInterrupted
      | otherwise = id

clientLoop :: ClientPtr -> IO (Handle, ReturnCode)
clientLoop (ClientPtr {..}) = do
  randomFailure loopState
  logPrint 13 "Client Loop" $ "Received client loop call"
  loopItem <- atomically $ do
    state <- readTVar loopState
    case state of 
      StateHealthy -> do
        loopItem <- tryReadTQueue handleQueue
        case loopItem of
          Just (h,rm,s,f) -> do
            when rm $
              modifyTVar handleList (delete h)
            return $ Right (f, h, s)
          _ -> do
            handles <- readTVar handleList
            case handles of 
              [] -> return $ Left LoopNonePending
              _  -> return $ Left LoopTimeout
      StateTransient rc -> do
        writeTVar loopState StateHealthy
        return $ Left rc
      StatePermanent rc -> do
        return $ Left $ rc
  case loopItem of
    Left rc -> do
      logPrint 13 "Client Loop" $ "Returning code: " ++ show rc
      return (-1, rc)
    Right (f, h, s) -> do
      logPrint 13 "Client Loop" $ "Running handle writer for handle: " ++ show h
      f
      logPrint 13 "Client Loop" $ "Returning handle " ++ show h 
                                ++ " with return code: " ++ show s
      return (h, s)

clientDeferredCall :: ClientPtr -> IORef String -> IO Handle
clientDeferredCall = clientCallBase Deferred

clientIteratorCall :: ClientPtr -> IORef String -> IO Handle
clientIteratorCall ptr ref = do
  nsquared <- randomRIO (1,16) :: IO Double
  let n = floor $ sqrt nsquared 
  clientCallBase (Iterator n) ptr ref

data Call = Deferred | Iterator Int

instance Show Call where
  show (Deferred) = "Deferred"
  show (Iterator _) = "Iterator"

clientCallBase :: Call -> ClientPtr -> IORef String -> IO Handle
clientCallBase call ptr outRef = do
  logPrint 11 "Client Call" $ "Received " ++ show call ++ " call"
  let n = case call of 
            Deferred   -> 1
            Iterator k -> k
  values <- newIORef [1..n]
  handle <- allocHandle ptr
  let msg i = case call of
        Deferred -> show i
        Iterator _ -> show i ++ "/" ++ show n
      writeValue = do
        logPrint 11 "Client Call" $ "Writing value to IORef for handle: " ++ show handle
        xs <- readIORef values
        case null xs of
          True  -> logPrint 0 "clientCallBase" "ERROR! Executed iterator too many times."
          False -> do
            let value = pad 3 ' ' (show handle)
                        ++ " - "
                        ++ replicate handle ' ' 
                        ++ pad 5 ' ' (msg (head xs))
            writeIORef outRef value
            writeIORef values (tail xs)
      completionLoop i
        | Deferred <- call = do 
            logPrint 15 "Coordinator" $ "Value ready to be written for handle: " ++ show handle
            handleReady handle True LoopSuccess writeValue ptr
        | i <= 0 = do
            logPrint 15 "Coordinator" $ "Iterator complete for handle: " ++ show handle
            handleReady handle True LoopSearchDone (return ()) ptr
        | otherwise = do
            logPrint 15 "Coordinator" $ "Value ready to be written for handle: " ++ show handle
            handleReady handle False LoopSuccess writeValue ptr
            randomDelay
            completionLoop $ i - 1
  void . forkIO $ (randomDelay >> completionLoop n)
  return handle
