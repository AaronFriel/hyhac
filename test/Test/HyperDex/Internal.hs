module Test.HyperDex.Internal (internalTests)
  where

import Test.Framework (testGroup, Test)
import Test.Framework.Providers.HUnit
import Test.HUnit hiding (Test)

import Database.HyperDex.Internal
import Foreign.Ptr

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
  client <- hyperclientCreate "127.0.0.1" 1982
  addSpaceResult <- hyperclientAddSpace client defaultSpace
  assertEqual "Add space: " HyperclientSuccess addSpaceResult
  print addSpaceResult
  removeSpaceResult <- hyperclientRemoveSpace client "profiles"
  assertEqual "Add space: " HyperclientSuccess removeSpaceResult
  print removeSpaceResult

internalTests :: Test
internalTests = testGroup "Internal API Tests"
                  [ canCreateAndRemoveSpaces
                  ]
