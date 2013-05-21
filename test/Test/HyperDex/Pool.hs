{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}

module Test.HyperDex.Pool ( poolTests )
  where

import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit hiding (Test)
import Control.Monad

import Control.Concurrent (threadDelay)

import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck hiding (NonEmpty)
import qualified Test.QuickCheck.Monadic as QC

import Test.HyperDex.Util

import Data.Text (Text)
import Data.Text.Encoding
import Data.ByteString.Char8 (ByteString, unpack)

import Database.HyperDex
import Database.HyperDex.Utf8

import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)
import qualified Data.Map as Map

import Data.Int
import Data.Ratio
import Data.Monoid
import Data.Serialize

import Data.Pool

import Debug.Trace

defaultHost :: Text
defaultHost = "127.0.0.1"

defaultPort :: Int
defaultPort = 1982

defaultSpace :: Text
defaultSpace = "profiles"

defaultSpaceDesc :: Text
defaultSpaceDesc = makeSpaceDesc defaultSpace

makeSpaceDesc :: Text -> Text
makeSpaceDesc name =
  "space "<>name<>"                         \n\
  \key username                             \n\
  \attributes                               \n\
  \   string first,                         \n\
  \   string last,                          \n\
  \   float score,                          \n\
  \   int profile_views,                    \n\
  \   list(string) pending_requests,        \n\
  \   list(float) rankings,                 \n\
  \   list(int) todolist,                   \n\
  \   set(string) hobbies,                  \n\
  \   set(float) imonafloat,                \n\
  \   set(int) friendids,                   \n\
  \   map(string, string) unread_messages,  \n\
  \   map(string, int) upvotes,             \n\
  \   map(string, float) friendranks,       \n\
  \   map(int, string) posts,               \n\
  \   map(int, int) friendremapping,        \n\
  \   map(int, float) intfloatmap,          \n\
  \   map(float, string) still_looking,     \n\
  \   map(float, int) for_a_reason,         \n\
  \   map(float, float) for_float_keyed_map \n\
  \create 10 partitions                     \n\
  \tolerate 2 failures"

withDefaultHost :: (Client -> IO a) -> IO a
withDefaultHost f = do
  client <- connect' defaultHost defaultPort
  res <- f client
  close client
  return res

withDefaultHostQC :: (Client -> QC.PropertyM IO a) -> QC.PropertyM IO a
withDefaultHostQC f = do
  client <- QC.run $ connect' defaultHost defaultPort
  res <- f client
  QC.run $ close client
  return res

cleanupSpace :: Text -> IO ()
cleanupSpace space =
  withDefaultHost $ \client -> do
    _ <- removeSpace client space
    return ()

canCreateSpace :: Test
canCreateSpace = testCase "Can create a space" $ do
  withDefaultHost $ \client -> do
    removeSpace client defaultSpace
    threadDelay 1000000
    addSpaceResult <- addSpace client defaultSpaceDesc
    threadDelay 1000000
    assertEqual "Add space: " HyperclientSuccess addSpaceResult

canRemoveSpace :: Test
canRemoveSpace = testCase "Can remove a space" $ do
  withDefaultHost $ \client -> do
    threadDelay 1000000
    removeSpaceResult <- removeSpace client defaultSpace
    assertEqual "Remove space: " HyperclientSuccess removeSpaceResult

testCanStoreLargeObject :: Pool Client -> Test
testCanStoreLargeObject clientPool = testCase "Can store a large object" $ do
  withDefaultHost $ \client -> do
    let attrs :: [Attribute]
        attrs =
          [ mkAttributeUtf8 "first"               (""        :: ByteString                 )
          , mkAttributeUtf8 "last"                (""        :: ByteString                 )
          , mkAttributeUtf8 "score"               (0.0       :: Double                     )
          , mkAttributeUtf8 "profile_views"       (0         :: Int64                      )
          , mkAttributeUtf8 "pending_requests"    ([]        :: [ByteString]               )
          , mkAttributeUtf8 "rankings"            ([]        :: [Double]                   )
          , mkAttributeUtf8 "todolist"            ([]        :: [Int64]                    )
          , mkAttributeUtf8 "hobbies"             (Set.empty :: Set ByteString             )
          , mkAttributeUtf8 "imonafloat"          (Set.empty :: Set Double                 )
          , mkAttributeUtf8 "friendids"           (Set.empty :: Set Int64                  )
          , mkAttributeUtf8 "unread_messages"     (Map.empty :: Map ByteString ByteString  )
          , mkAttributeUtf8 "upvotes"             (Map.empty :: Map ByteString Int64       )
          , mkAttributeUtf8 "friendranks"         (Map.empty :: Map ByteString Double      )
          , mkAttributeUtf8 "posts"               (Map.empty :: Map Int64      ByteString  )
          , mkAttributeUtf8 "friendremapping"     (Map.empty :: Map Int64      Int64       )
          , mkAttributeUtf8 "intfloatmap"         (Map.empty :: Map Int64      Double      )
          , mkAttributeUtf8 "still_looking"       (Map.empty :: Map Double     ByteString  )
          , mkAttributeUtf8 "for_a_reason"        (Map.empty :: Map Double     Int64       )
          , mkAttributeUtf8 "for_float_keyed_map" (Map.empty :: Map Double     Double      )
          ]
    result <- join $ withResource clientPool $ \client -> putAsyncAttr client defaultSpace "large" attrs
    assertEqual "Remove space: " (Right ()) result

getResult :: HyperSerialize a => Text -> Either ReturnCode [Attribute] -> Either String a
getResult attribute (Left returnCode) = Left $ "Failure, returnCode: " <> show returnCode
getResult attribute (Right attrList)  =
  case (filter (\a -> attrName a == encodeUtf8 attribute) attrList) of
          [x] -> case deserialize $ attrValue x of
            Left serializeError -> Left $ "Error deserializing: " <> serializeError
            Right value         -> Right value
          []  -> Left $ "No valid attribute, attributes list: " <> show attrList
          _   -> Left "More than one returned value"

putHyper :: HyperSerialize a => Pool Client -> Text -> ByteString -> Text -> a -> QC.PropertyM IO (Either ReturnCode ())
putHyper clientPool space key attribute value = do
    QC.run . join $ withResource clientPool $ \client -> putAsyncAttr client space key [mkAttributeUtf8 attribute value]

getHyper :: HyperSerialize a => Pool Client -> Text -> ByteString -> Text -> QC.PropertyM IO (Either String a)
getHyper clientPool space key attribute = do
    eitherAttrList <- QC.run . join $ withResource clientPool $ \client -> getAsyncAttr client space key
    let retValue = getResult attribute eitherAttrList 
    case retValue of
      Left err -> QC.run $ do
        putStrLn $ "getHyper encountered error: " <> show err
        putStrLn $ "Attribute: "
        putStrLn $ show . fmap (filter (\x -> decodeUtf8 (attrName x) == attribute)) $ eitherAttrList
      _ -> return ()
    return $ retValue

propCanStore :: (Show a, Eq a, HyperSerialize a) => Pool Client -> ByteString -> a 
                -> Text -> NonEmpty ByteString -> Property
propCanStore clientPool attribute input space (NonEmpty key) =
  QC.monadicIO $ do
    r1 <- putHyper clientPool space key (decodeUtf8 attribute) input
    eitherOutput <- getHyper clientPool space key (decodeUtf8 attribute)
    case eitherOutput of
      Right output -> do
        case input == output of
          True -> QC.assert True
          False -> do 
            QC.run $ do
              putStrLn $ "Failed to store value:"
              putStrLn $ "  space:  " <> show space
              putStrLn $ "  key:    " <> show key
              putStrLn $ "  attr:   " <> show attribute
              putStrLn $ "  input:  " <> show input
              putStrLn $ "  output: " <> show output
            QC.assert False
      Left reason  -> do 
        QC.run $ do
          putStrLn $ "Failed to retrieve value:"
          putStrLn $ "  space:  " <> show space
          putStrLn $ "  key:    " <> show key
          putStrLn $ "  attr:   " <> show attribute
          putStrLn $ "  reason: " <> show reason
        QC.assert False

createAction = do
  connect' defaultHost defaultPort

closeAction client = do
  close client

mkPool = createPool createAction closeAction 4 (fromRational $ 1%2) 10

testCanStoreDoubles :: Pool Client -> Test
testCanStoreDoubles clientPool =
  testProperty
    "Can round trip a floating point Double through HyperDex"
    $ \(value :: Double) -> propCanStore clientPool "score" value defaultSpace

testCanStoreIntegers :: Pool Client -> Test
testCanStoreIntegers clientPool =
  testProperty
    "Can round trip an integer through HyperDex"
    $ \(value :: Int64) -> propCanStore clientPool "profile_views" value defaultSpace

testCanStoreStrings :: Pool Client -> Test
testCanStoreStrings clientPool =
  testProperty
    "Can round trip a string through HyperDex"
    $ \(value :: ByteString) -> propCanStore clientPool "first" value defaultSpace

testCanStoreListOfStrings :: Pool Client -> Test
testCanStoreListOfStrings clientPool =
  testProperty
    "Can round trip a list of strings through HyperDex"
    $ \(value :: [ByteString]) -> propCanStore clientPool "pending_requests" value defaultSpace

testCanStoreListOfDoubles :: Pool Client -> Test
testCanStoreListOfDoubles clientPool =
  testProperty
    "Can round trip a list of floating point doubles through HyperDex"
    $ \(value :: [Double]) -> propCanStore clientPool "rankings" value defaultSpace

testCanStoreListOfIntegers :: Pool Client -> Test
testCanStoreListOfIntegers clientPool =
  testProperty
    "Can round trip a list of integers through HyperDex"
    $ \(value :: [Int64]) -> propCanStore clientPool "todolist" value defaultSpace

testCanStoreSetOfStrings :: Pool Client -> Test
testCanStoreSetOfStrings clientPool =
  testProperty
    "Can round trip a set of strings through HyperDex"
    $ \(value :: Set ByteString) -> propCanStore clientPool "hobbies" value defaultSpace

testCanStoreSetOfDoubles :: Pool Client -> Test
testCanStoreSetOfDoubles clientPool =
  testProperty
    "Can round trip a set of floating point doubles through HyperDex"
    $ \(value :: Set Double) -> propCanStore clientPool "imonafloat" value defaultSpace

testCanStoreSetOfIntegers :: Pool Client -> Test
testCanStoreSetOfIntegers clientPool =
  testProperty
    "Can round trip a set of integers through HyperDex"
    $ \(value :: Set Int64) -> propCanStore clientPool "friendids" value defaultSpace

testCanStoreMapOfStringsToStrings :: Pool Client -> Test
testCanStoreMapOfStringsToStrings clientPool =
  testProperty
    "Can round trip a map of strings to strings through HyperDex"
    $ \(value :: Map ByteString ByteString) -> propCanStore clientPool "unread_messages" value defaultSpace

testCanStoreMapOfStringsToIntegers :: Pool Client -> Test
testCanStoreMapOfStringsToIntegers clientPool =
  testProperty
    "Can round trip a map of strings to integers through HyperDex"
    $ \(value :: Map ByteString Int64) -> propCanStore clientPool "upvotes" value defaultSpace

testCanStoreMapOfStringsToDoubles :: Pool Client -> Test
testCanStoreMapOfStringsToDoubles clientPool =
  testProperty
    "Can round trip a map of strings to floating point doubles through HyperDex"
    $ \(value :: Map ByteString Double) -> propCanStore clientPool "friendranks" value defaultSpace

testCanStoreMapOfIntegersToStrings :: Pool Client -> Test
testCanStoreMapOfIntegersToStrings clientPool =
  testProperty
    "Can round trip a map of integers to strings through HyperDex"
    $ \(value :: Map Int64 ByteString) -> propCanStore clientPool "posts" value defaultSpace

testCanStoreMapOfIntegersToIntegers :: Pool Client -> Test
testCanStoreMapOfIntegersToIntegers clientPool =
  testProperty
    "Can round trip a map of integers to integers through HyperDex"
    $ \(value :: Map Int64 Int64) -> propCanStore clientPool "friendremapping" value defaultSpace

testCanStoreMapOfIntegersToDoubles :: Pool Client -> Test
testCanStoreMapOfIntegersToDoubles clientPool =
  testProperty
    "Can round trip a map of integers to floating point doubles through HyperDex"
    $ \(value :: Map Int64 Double) -> propCanStore clientPool "intfloatmap" value defaultSpace

testCanStoreMapOfDoublesToStrings :: Pool Client -> Test
testCanStoreMapOfDoublesToStrings clientPool =
  testProperty
    "Can round trip a map of floating point doubles to strings through HyperDex"
    $ \(value :: Map Double ByteString) -> propCanStore clientPool "still_looking" value defaultSpace

testCanStoreMapOfDoublesToIntegers :: Pool Client -> Test
testCanStoreMapOfDoublesToIntegers clientPool =
  testProperty
    "Can round trip a map of floating point doubles to integers through HyperDex"
    $ \(value :: Map Double Int64) -> propCanStore clientPool "for_a_reason" value defaultSpace

testCanStoreMapOfDoublesToDoubles :: Pool Client -> Test
testCanStoreMapOfDoublesToDoubles clientPool =
  testProperty
    "Can round trip a map of floating point doubles to floating point doubles through HyperDex"
    $ \(value :: Map Double Double) -> propCanStore clientPool "for_float_keyed_map" value defaultSpace

canCreateAndRemoveSpaces :: Test
canCreateAndRemoveSpaces = do
  testGroup "Can create and remove space" [ canCreateSpace, canRemoveSpace ]

poolTests :: Test
poolTests = buildTest $ do
  clientPool <- mkPool 
  let tests = plusTestOptions 
              (mempty { topt_maximum_generated_tests = Just 1000
                      , topt_maximum_test_size = Just 64
                      })
              $ testGroup "Pool API Tests"
                [ testCanStoreLargeObject clientPool
                , testCanStoreIntegers clientPool
                , testCanStoreStrings clientPool
                , testCanStoreDoubles clientPool
                , testCanStoreListOfStrings clientPool
                , testCanStoreListOfDoubles clientPool
                , testCanStoreListOfIntegers clientPool
                , testCanStoreSetOfStrings clientPool
                , testCanStoreSetOfDoubles clientPool
                , testCanStoreSetOfIntegers clientPool
                , testCanStoreMapOfStringsToStrings clientPool
                , testCanStoreMapOfStringsToIntegers clientPool
                , testCanStoreMapOfStringsToDoubles clientPool
                , testCanStoreMapOfIntegersToStrings clientPool
                , testCanStoreMapOfIntegersToIntegers clientPool
                , testCanStoreMapOfIntegersToDoubles clientPool
                , testCanStoreMapOfDoublesToStrings clientPool
                , testCanStoreMapOfDoublesToIntegers clientPool
                , testCanStoreMapOfDoublesToDoubles clientPool
                ]
  return tests
