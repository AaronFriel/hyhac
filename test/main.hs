{-# LANGUAGE ExistentialQuantification, TypeOperators #-}

module Main ( main ) where

import Test.HyperDex.Space

import Database.HyperDex.Admin

import Control.Concurrent (threadDelay, forkFinally)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Monad

import Test.Framework

import Test.HyperDex.Shared (sharedTests)
import Test.HyperDex.Pool (poolTests)
import Test.HyperDex.CBString (cBStringTests)

-- TODO:
-- import Test.HyperDex.Simulator

tests :: Test
tests =
  testGroup "hyhac-tests"
  [ testGroup "hyperdex"
    [ addSpaceTest
    , poolTests
    , sharedTests
    , removeSpaceTest
    ]
  , cBStringTests
  ]

preamble :: IO ()
preamble = return ()

postscript :: IO ()
postscript = do
  client <- connect defaultConnectInfo
  -- Force remove space in case it was not deleted.
  _ <- join $ removeSpace client defaultSpace
  close client

main = do
  preamble
  -- This hackish solution allows us to get around the fact that defaultMain
  -- will call exitWith. We fork off the computation and wrap it with forkFinally
  -- and wait on an MVar before calling our cleanup operation.
  wait <- newEmptyMVar
  _ <- forkFinally (defaultMain [tests]) (const $ putMVar wait ())
  takeMVar wait
  postscript
