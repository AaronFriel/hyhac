{-# LANGUAGE OverloadedStrings, ScopedTypeVariables, ExistentialQuantification #-}

module Test.HyperDex.Internal (internalTests)
  where

import Test.HyperDex.Space

import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit hiding (Test)
import Control.Monad

import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck hiding (NonEmpty)
import qualified Test.QuickCheck.Monadic as QC

import Test.HyperDex.Util

import Data.Text (Text)
import Data.Text.Encoding
import Data.ByteString.Char8 (ByteString)

import Database.HyperDex
import Database.HyperDex.Utf8

import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)
import qualified Data.Map as Map

import Data.Int
import Data.Monoid

testCanStoreLargeObject :: Test
testCanStoreLargeObject = testCase "Can store a large object" $ do
  client <- connect' defaultHost defaultPort
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
  result <- join $ putAsyncAttr client defaultSpace "large" attrs
  close client
  assertEqual "Remove space: " (Right ()) result


getResult :: HyperSerialize a => Text -> Either ReturnCode [Attribute] -> Either String a
getResult _         (Left returnCode) = Left $ "Failure, returnCode: " <> show returnCode
getResult attribute (Right attrList)  =
  case (filter (\a -> attrName a == encodeUtf8 attribute) attrList) of
          [x] -> case deserialize $ attrValue x of
            Left serializeError -> Left $ "Error deserializing: " <> serializeError
            Right value         -> Right value
          []  -> Left $ "No valid attribute, attributes list: " <> show attrList
          _   -> Left "More than one returned value"

putHyper :: HyperSerialize a => Client -> Text -> ByteString -> Text -> a -> QC.PropertyM IO (Either ReturnCode ())
putHyper client space key attribute value = do
    QC.run . join $ putAsyncAttr client space key [mkAttributeUtf8 attribute value]

getHyper :: HyperSerialize a => Client -> Text -> ByteString -> Text -> QC.PropertyM IO (Either String a)
getHyper client space key attribute = do
    eitherAttrList <- QC.run . join $ getAsyncAttr client space key
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
    _ <- putHyper client space key (decodeUtf8 attribute) input
    eitherOutput <- getHyper client space key (decodeUtf8 attribute)
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

internalTests :: Test
internalTests = testGroup "non-pooled-api-tests"
                  [ testCanStoreLargeObject
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
                  ]
