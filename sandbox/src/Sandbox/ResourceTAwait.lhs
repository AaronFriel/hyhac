---
readme: |

  This is a Literate Haskell document that uses Pandoc-flavored Markdown. This
  has a few implications:

  1. GHC will discard most lines of text. The lines that it reads in for input
     fall into three categies:
    a. Lines beginning with `#` are C preprocessor statements and if invalid,
       will be considered errors. (This means that Markdown headings with a # in
       the first column are errors.)
    b. Lines beginning with a `>` are Haskell, discarding the `>` prefix.
    c. Any line that is between the verbatim lines `\begin{code}` and
       `\end{code}` are Haskell
  2. This block is YAML metadata used to inform producing documents via Pandoc,
     it is enabled by the `yaml_metadata_block` extension.
  3. This document may also contain embedded LaTeX.

title:  'ResourceT Await'
subtitle: 'Asynchronous Event Loop Resource Handling'
author:
- name: Aaron Friel
  affiliation: University of Northern Iowa
  email: mayreply@aaronfriel.com
license: BSD3
classoption: [nocopyrightspace,twocolumn,9pt]
listings: true
...

\begin{code}

{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeHoles #-}

module Sandbox.ResourceTAwait
 where

import Sandbox.Hyhac
import Sandbox.SafePrint
import Sandbox.Util

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import System.Random

import Chaos.Doomsday

-- | Given an integer 'n', returns a signal, and a wait function.
-- The wait will only return when the signal has been called 'n'
-- times. Only works once.
makeSemLock :: Int -> IO (IO (), IO ())
makeSemLock n = do
  var <- newTVarIO 0
  lock <- newEmptyTMVarIO
  let signal = atomically $ do
        value <- readTVar var
        let newValue = value + 1
        if newValue >= n
        then void $ tryPutTMVar lock ()
        else return ()
        writeTVar var newValue
  let wait = atomically $ readTMVar lock
  when (n <= 0) $ void $ atomically $ tryPutTMVar lock ()
  return (signal, wait)

data Settings = Settings
  { numberOfCalls :: Int
  , logVerbosity :: Int
  , logSynchronously :: Bool
  , doomsdayClock :: Maybe Int
  }

main :: IO ()
main = do
  void $ newStdGen
  numCalls <- randomRIO (5,100)
  run $ Settings numCalls 7 False Nothing

test :: Int -> Int -> Bool -> Maybe Int -> IO ()
test n v s d = run $ Settings n v s d

run :: Settings -> IO ()
run (Settings {..}) = do
  setVerbosity logVerbosity
  setSynchronousLogging logSynchronously
  maybe stopDoomsday setDoomsdayClock doomsdayClock
  setDoomsdayClockRepeat doomsdayClock
  prng <- getStdGen
  let randomValues = randoms prng :: [Double]
  (signal, wait) <- makeSemLock numberOfCalls
  client <- suspendDoomsday connect

  let padn = pad (length $ show numberOfCalls) ' ' . show

  flip mapM_ (zip [1..numberOfCalls] randomValues) $
    \(n,r) -> forkIO $ do
      case () of
        _ | r < 0.5 -> do
              futureResult <- hyhacDeferred client
              result <- futureResult
              logPrint 1 ("Deferred result " ++ padn n) $ show result
              signal
          | otherwise -> do
              stream <- hyhacIterator client
              streamSequence_ (logPrint 1 ("Iterator result " ++ padn n) . show) stream
              signal
  wait
  threadDelayms 50
  syncLogPrint 0 "run" $ "All " ++ show numberOfCalls ++ " transactions complete."

\end{code}
