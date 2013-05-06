module Test.HyperDex.Internal (internalTests)
  where

import Test.Framework (testGroup, Test)
import Test.Framework.Providers.HUnit
import Test.HUnit hiding (Test)

import Database.HyperDex.Internal

import Data.Int
import Foreign.C
import Foreign.Ptr

defaultHost :: String
defaultHost = "127.0.0.1"

defaultPort :: Int16
defaultPort = 1982

defaultSpaceName :: String
defaultSpaceName = "profiles"

defaultSpace :: String
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
withDefaultHost f =
  withCString defaultHost $ \host -> do
    client <- hyperclientCreate host defaultPort
    res <- f client
    hyperclientDestroy client
    return res

canCreateSpace :: Test
canCreateSpace = testCase "Can create a space" $ do
  withDefaultHost $ \client -> do
    withCString defaultSpace $ \space -> do
      addSpaceResult <- hyperclientAddSpace client space
      assertEqual "Add space: " HyperclientSuccess addSpaceResult


canRemoveSpace :: Test
canRemoveSpace = testCase "Can remove a space" $ do
  withDefaultHost $ \client -> do
    withCString defaultSpaceName $ \spaceName -> do
      removeSpaceResult <- hyperclientRemoveSpace client spaceName
      assertEqual "Remove space: " HyperclientSuccess removeSpaceResult

canCreateAndRemoveSpaces :: Test
canCreateAndRemoveSpaces = testGroup "Can create and remove space" [ canCreateSpace, canRemoveSpace ]

internalTests :: Test
internalTests = testGroup "Internal API Tests"
                  [ canCreateAndRemoveSpaces
                  ]
