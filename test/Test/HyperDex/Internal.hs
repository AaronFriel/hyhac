module Test.HyperDex.Internal (internalTests)
  where

import Test.Framework (testGroup, Test)
import Test.Framework.Providers.HUnit
import Test.HUnit hiding (Test)

import Database.HyperDex.Internal
import Foreign.C
import Foreign.Ptr

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

canCreateAndRemoveSpaces :: Test
canCreateAndRemoveSpaces = testCase "Can create and remove spaces" $ do
  withCString "127.0.0.1" $ \host -> do
    client <- hyperclientCreate host 1982
    withCString defaultSpace $ \space -> do
      addSpaceResult <- hyperclientAddSpace client space
      assertEqual "Add space: " HyperclientSuccess addSpaceResult
    withCString defaultSpaceName $ \spaceName -> do
      removeSpaceResult <- hyperclientRemoveSpace client spaceName
      assertEqual "Remove space: " HyperclientSuccess removeSpaceResult

internalTests :: Test
internalTests = testGroup "Internal API Tests"
                  [   canCreateAndRemoveSpaces
                  ]
