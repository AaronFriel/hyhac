{-# LANGUAGE OverloadedStrings #-}

module Test.HyperDex.Internal (internalTests)
  where

import Test.Framework (testGroup, Test)
import Test.Framework.Providers.HUnit
import Test.HUnit hiding (Test)

import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck
import Test.QuickCheck.Monadic

import Database.HyperDex.Internal
import Database.HyperDex.Internal.Util

import Data.ByteString (ByteString (..))
import Data.Int
import Foreign.C
import Foreign.Ptr

defaultHost :: ByteString
defaultHost = "127.0.0.1"

defaultPort :: Int16
defaultPort = 1982

defaultSpaceName :: ByteString
defaultSpaceName = "profiles"

defaultSpace :: ByteString
defaultSpace =
  "space profiles                           \n\
  \key username                             \n\
  \attributes                               \n\
  \   string first,                         \n\
  \   string last,                          \n\
  \   int profile_views,                    \n\
  \   list(string) pending_requests,        \n\
  \   set(string) hobbies,                  \n\
  \   map(string, string) unread_messages,  \n\
  \   map(string, int) upvotes              \n\
  \subspace first, last                     \n\
  \subspace profile_views"

withDefaultHost :: (HyperclientPtr -> IO a) -> IO a
withDefaultHost f = do
  client <- hyperclientCreate defaultHost defaultPort
  res <- f client
  hyperclientDestroy client
  return res

canCreateSpace :: Test
canCreateSpace = testCase "Can create a space" $ do
  withDefaultHost $ \client -> do
    addSpaceResult <- hyperclientAddSpace client defaultSpace
    assertEqual "Add space: " HyperclientSuccess addSpaceResult

canRemoveSpace :: Test
canRemoveSpace = testCase "Can remove a space" $ do
  withDefaultHost $ \client -> do
    removeSpaceResult <- hyperclientRemoveSpace client defaultSpaceName
    assertEqual "Remove space: " HyperclientSuccess removeSpaceResult

canCreateAndRemoveSpaces :: Test
canCreateAndRemoveSpaces = testGroup "Can create and remove space" [ canCreateSpace, canRemoveSpace ]

internalTests :: Test
internalTests = testGroup "Internal API Tests"
                  [ canCreateAndRemoveSpaces
                  ]
