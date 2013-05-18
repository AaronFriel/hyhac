{-# LANGUAGE OverloadedStrings, ScopedTypeVariables, ExistentialQuantification #-}

module Test.HyperDex.Internal (internalTests)
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
import Database.HyperDex.Internal.Hyperdata

import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)
import qualified Data.Map as Map

import Data.Int
import Data.Monoid
import Data.Serialize

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
  \tolerate 1 failures"

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

testCanStoreLargeObject :: Test
testCanStoreLargeObject = testCase "Can store a large object" $ do
  withDefaultHost $ \client -> do
    let attrs :: [Attribute]
        attrs =
          [ mkAttribute ("first"               :: String) (""        :: ByteString                 )
          , mkAttribute ("last"                :: String) (""        :: ByteString                 )
          , mkAttribute ("score"               :: String) (0.0       :: Double                     )
          , mkAttribute ("profile_views"       :: String) (0         :: Int64                      )
          , mkAttribute ("pending_requests"    :: String) ([]        :: [ByteString]               )
          , mkAttribute ("rankings"            :: String) ([]        :: [Double]                   )
          , mkAttribute ("todolist"            :: String) ([]        :: [Int64]                    )
          , mkAttribute ("hobbies"             :: String) (Set.empty :: Set ByteString             )
          , mkAttribute ("imonafloat"          :: String) (Set.empty :: Set Double                 )
          , mkAttribute ("friendids"           :: String) (Set.empty :: Set Int64                  )
          , mkAttribute ("unread_messages"     :: String) (Map.empty :: Map ByteString ByteString  )
          , mkAttribute ("upvotes"             :: String) (Map.empty :: Map ByteString Int64       )
          , mkAttribute ("friendranks"         :: String) (Map.empty :: Map ByteString Double      )
          , mkAttribute ("posts"               :: String) (Map.empty :: Map Int64      ByteString  )
          , mkAttribute ("friendremapping"     :: String) (Map.empty :: Map Int64      Int64       )
          , mkAttribute ("intfloatmap"         :: String) (Map.empty :: Map Int64      Double      )
          , mkAttribute ("still_looking"       :: String) (Map.empty :: Map Double     ByteString  )
          , mkAttribute ("for_a_reason"        :: String) (Map.empty :: Map Double     Int64       )
          , mkAttribute ("for_float_keyed_map" :: String) (Map.empty :: Map Double     Double      )
          ]
    result <- join $ putAsyncAttr client defaultSpace "large" attrs
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

putHyper :: HyperSerialize a => Client -> Text -> Text -> Text -> a -> QC.PropertyM IO (Either ReturnCode ())
putHyper client space key attribute value = do
    QC.run $ putStrLn "Test put!"
    QC.run . join $ putAsyncAttr client space key [mkAttribute attribute value]
    
getHyper :: HyperSerialize a => Client -> Text -> Text -> Text -> QC.PropertyM IO (Either String a)
getHyper client space key attribute = do
    QC.run $ putStrLn "Test get!"
    eitherAttrList <- QC.run . join $ getAsyncAttr client space key
    QC.run $ putStrLn "Test getAttr!"
    let retValue = getResult attribute eitherAttrList 
    case retValue of
      Left err -> QC.run $ do
        putStrLn $ "getHyper encountered error: " <> show err
        putStrLn $ "Attribute: "
        putStrLn $ show . fmap (filter (\x -> decodeUtf8 (attrName x) == attribute)) $ eitherAttrList
      _ -> return ()
    return $ retValue

propCanStore :: (Show a, Eq a, HyperSerialize a) => Client -> ByteString -> a 
                -> Text -> NonEmpty ByteString -> Property
propCanStore client attribute input space (NonEmpty key) =
  QC.monadicIO $ do
    QC.run $ putStrLn "Test canStore!"
    r1 <- putHyper client space (decodeUtf8 key) (decodeUtf8 attribute) input
    eitherOutput <- getHyper client space (decodeUtf8 key) (decodeUtf8 attribute)
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

testCanStoreDoubles :: Test
testCanStoreDoubles = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a floating point Double through HyperDex"
            $ \(value :: Double) -> propCanStore client "score" value defaultSpace
    return (test, close client)

testCanStoreIntegers :: Test
testCanStoreIntegers = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip an integer through HyperDex"
            $ \(value :: Int64) -> propCanStore client "profile_views" value defaultSpace
    return (test, close client)

testCanStoreStrings :: Test
testCanStoreStrings = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a string through HyperDex"
            $ \(value :: ByteString) -> propCanStore client "first" value defaultSpace
    return (test, close client)

testCanStoreListOfStrings :: Test
testCanStoreListOfStrings = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a list of strings through HyperDex"
            $ \(value :: [ByteString]) -> propCanStore client "pending_requests" value defaultSpace
    return (test, close client)

testCanStoreListOfDoubles :: Test
testCanStoreListOfDoubles = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a list of floating point doubles through HyperDex"
            $ \(value :: [Double]) -> propCanStore client "rankings" value defaultSpace
    return (test, close client)

testCanStoreListOfIntegers :: Test
testCanStoreListOfIntegers = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a list of integers through HyperDex"
            $ \(value :: [Int64]) -> propCanStore client "todolist" value defaultSpace
    return (test, close client)

testCanStoreSetOfStrings :: Test
testCanStoreSetOfStrings = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a set of strings through HyperDex"
            $ \(value :: Set ByteString) -> propCanStore client "hobbies" value defaultSpace
    return (test, close client)

testCanStoreSetOfDoubles :: Test
testCanStoreSetOfDoubles = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a set of floating point doubles through HyperDex"
            $ \(value :: Set Double) -> propCanStore client "imonafloat" value defaultSpace
    return (test, close client)

testCanStoreSetOfIntegers :: Test
testCanStoreSetOfIntegers = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a set of integers through HyperDex"
            $ \(value :: Set Int64) -> propCanStore client "friendids" value defaultSpace
    return (test, close client)

testCanStoreMapOfStringsToStrings :: Test
testCanStoreMapOfStringsToStrings = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a map of strings to strings through HyperDex"
            $ \(value :: Map ByteString ByteString) -> propCanStore client "unread_messages" value defaultSpace
    return (test, close client)

testCanStoreMapOfStringsToIntegers :: Test
testCanStoreMapOfStringsToIntegers = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a map of strings to integers through HyperDex"
            $ \(value :: Map ByteString Int64) -> propCanStore client "upvotes" value defaultSpace
    return (test, close client)

testCanStoreMapOfStringsToDoubles :: Test
testCanStoreMapOfStringsToDoubles = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a map of strings to floating point doubles through HyperDex"
            $ \(value :: Map ByteString Double) -> propCanStore client "friendranks" value defaultSpace
    return (test, close client)

testCanStoreMapOfIntegersToStrings :: Test
testCanStoreMapOfIntegersToStrings = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a map of integers to strings through HyperDex"
            $ \(value :: Map Int64 ByteString) -> propCanStore client "posts" value defaultSpace
    return (test, close client)

testCanStoreMapOfIntegersToIntegers :: Test
testCanStoreMapOfIntegersToIntegers = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a map of integers to integers through HyperDex"
            $ \(value :: Map Int64 Int64) -> propCanStore client "friendremapping" value defaultSpace
    return (test, close client)

testCanStoreMapOfIntegersToDoubles :: Test
testCanStoreMapOfIntegersToDoubles = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a map of integers to floating point doubles through HyperDex"
            $ \(value :: Map Int64 Double) -> propCanStore client "intfloatmap" value defaultSpace
    return (test, close client)

testCanStoreMapOfDoublesToStrings :: Test
testCanStoreMapOfDoublesToStrings = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a map of floating point doubles to strings through HyperDex"
            $ \(value :: Map Double ByteString) -> propCanStore client "still_looking" value defaultSpace
    return (test, close client)

testCanStoreMapOfDoublesToIntegers :: Test
testCanStoreMapOfDoublesToIntegers = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a map of floating point doubles to integers through HyperDex"
            $ \(value :: Map Double Int64) -> propCanStore client "for_a_reason" value defaultSpace
    return (test, close client)

testCanStoreMapOfDoublesToDoubles :: Test
testCanStoreMapOfDoublesToDoubles = buildTestBracketed $ do
    client <- connect' defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a map of floating point doubles to floating point doubles through HyperDex"
            $ \(value :: Map Double Double) -> propCanStore client "for_float_keyed_map" value defaultSpace
    return (test, close client)

canCreateAndRemoveSpaces :: Test
canCreateAndRemoveSpaces = do
  testGroup "Can create and remove space" [ canCreateSpace, canRemoveSpace ]

internalTests :: Test
internalTests = mutuallyExclusive $
                plusTestOptions 
                (mempty { topt_maximum_generated_tests = Just 1000
                        , topt_maximum_test_size = Just 64
                        })
                $ testGroup "Internal API Tests"
                  [ canCreateSpace
                  , testCanStoreLargeObject
                  , testCanStoreIntegers
                  , testCanStoreStrings
                  , testCanStoreDoubles
                  , testCanStoreListOfStrings
                  , testCanStoreListOfDoubles
                  , testCanStoreListOfIntegers
                  , testCanStoreSetOfStrings
                  , testCanStoreSetOfDoubles
                  , testCanStoreSetOfIntegers
                  , testCanStoreMapOfStringsToStrings
                  , testCanStoreMapOfStringsToIntegers
                  , testCanStoreMapOfStringsToDoubles
                  , testCanStoreMapOfIntegersToStrings
                  , testCanStoreMapOfIntegersToIntegers
                  , testCanStoreMapOfIntegersToDoubles
                  , testCanStoreMapOfDoublesToStrings
                  , testCanStoreMapOfDoublesToIntegers
                  , testCanStoreMapOfDoublesToDoubles
                  , canRemoveSpace
                  ]
