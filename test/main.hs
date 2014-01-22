{-# LANGUAGE ExistentialQuantification, TypeOperators #-}

module Main ( main ) where

import Test.HyperDex.Space

import Database.HyperDex.Admin

import Control.Concurrent (threadDelay, forkFinally)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)

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
    [ poolTests
    , sharedTests
    ]
  , cBStringTests
  ]

preamble :: IO ()
preamble = do
  client <- connect defaultConnectInfo
  removeSpace client defaultSpace
  threadDelay 250000
  addSpace client defaultSpaceDesc
  threadDelay 250000
  close client

postscript :: IO ()
postscript = do
  client <- connect defaultConnectInfo
  removeSpace client defaultSpace
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
