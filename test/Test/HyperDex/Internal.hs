{-# LANGUAGE OverloadedStrings #-}

module Test.HyperDex.Internal (internalTests)
  where

import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit hiding (Test)
import Control.Monad

import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck
import qualified Test.QuickCheck.Monadic as QC

import Test.HyperDex.Util

import Database.HyperDex.Internal

import Data.Serialize (runPut, runGet, put, get)

import Data.Int
import Data.Monoid

defaultHost :: ByteString
defaultHost = "127.0.0.1"

defaultPort :: Int16
defaultPort = 1982

defaultSpace :: ByteString
defaultSpace = "profiles"

defaultSpaceDesc :: ByteString
defaultSpaceDesc = makeSpaceDesc defaultSpace

makeSpaceDesc :: ByteString -> ByteString
makeSpaceDesc name =
  "space "<>name<>"                         \n\
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

withDefaultHost :: (Client -> IO a) -> IO a
withDefaultHost f = do
  client <- makeClient defaultHost defaultPort
  res <- f client
  closeClient client
  return res

withDefaultHostQC :: (Client -> QC.PropertyM IO a) -> QC.PropertyM IO a
withDefaultHostQC f = do
  client <- QC.run $ makeClient defaultHost defaultPort
  res <- f client
  QC.run $ closeClient client
  return res

cleanupSpace :: ByteString -> IO ()
cleanupSpace space =
  withDefaultHost $ \client -> do
    _ <- removeSpace client space
    return ()

canCreateSpace :: Test
canCreateSpace = testCase "Can create a space" $ do
  withDefaultHost $ \client -> do
    addSpaceResult <- addSpace client defaultSpaceDesc
    assertEqual "Add space: " HyperclientSuccess addSpaceResult

canRemoveSpace :: Test
canRemoveSpace = testCase "Can remove a space" $ do
  withDefaultHost $ \client -> do
    removeSpaceResult <- removeSpace client defaultSpace
    assertEqual "Remove space: " HyperclientSuccess removeSpaceResult

putInteger ::  Client -> ByteString -> ByteString -> ByteString -> Int64 -> QC.PropertyM IO HyperclientReturnCode
putInteger client space key attribute value = do
    let serializedValue = runPut . put . Hyper $ value
    QC.run . join $ hyperPut client space key [Attribute attribute serializedValue HyperdatatypeInt64]

getInteger ::  Client -> ByteString -> ByteString -> QC.PropertyM IO (HyperclientReturnCode, Either String Int64)
getInteger client space key = do
    (returnCode, attrList) <- QC.run . join $ hyperGet client space key
    let value =
          case (filter (\a -> attrName a == "profile_views") attrList) of
            []  -> Left "No returned value"
            [x] -> fmap unHyper . runGet get $ attrValue x
            _   -> Left "More than one returned value"
    return (returnCode, value)

propCanStoreIntegers :: Client -> ByteString -> (LowerAscii ByteString) -> Int64 -> Property
propCanStoreIntegers client space (LowerAscii key) input =
  QC.monadicIO $ do
    r1 <- putInteger client space key "profile_views" input
    (r2, eitherOutput) <- getInteger client space key
    case eitherOutput of
      Right output -> return $ input == output
      Left _       -> return False

testCanStoreIntegers :: Test
testCanStoreIntegers = buildTestBracketed $ do
    client <- makeClient defaultHost defaultPort
    let space = "testintegers"
    addSpace client (makeSpaceDesc space)
    let test = testProperty "Can round trip an integer through HyperDex"
                            (propCanStoreIntegers client space)
    return (test, closeClient client >> cleanupSpace space)

canCreateAndRemoveSpaces :: Test
canCreateAndRemoveSpaces = do
  testGroup "Can create and remove space" [ canCreateSpace, canRemoveSpace ]

internalTests :: Test
internalTests = mutuallyExclusive 
                $ plusTestOptions 
                  (mempty { topt_maximum_generated_tests = Just 1000
                          , topt_timeout = Just (Just 250000) -- microseconds before considering failure, 250ms
                          })
                $ testGroup "Internal API Tests"
                  [ testCanStoreIntegers
                  ]