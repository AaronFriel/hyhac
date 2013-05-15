{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}

module Test.HyperDex.Internal (internalTests)
  where

import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit hiding (Test)
import Control.Monad

import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck hiding (NonEmpty)
import qualified Test.QuickCheck.Monadic as QC

import Test.HyperDex.Util

import Data.ByteString.Char8 (unpack)

import Database.HyperDex.Internal

import Data.Serialize (runPut, runGet, put, get)

import Data.Int
import Data.Monoid
import Data.Serialize

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
  \   float score,                          \n\
  \   int profile_views,                    \n\
  \   list(string) pending_requests,        \n\
  \   list(float) rankings,                 \n\
  \   list(int) todolist,                   \n\
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

getResult :: HyperSerialize a => ByteString -> Either HyperclientReturnCode [Attribute] -> Either String a
getResult attribute (Left returnCode) = Left $ "Failure, returnCode: " <> show returnCode
getResult attribute (Right attrList)  =
  case (filter (\a -> attrName a == attribute) attrList) of
          [x] -> case fmap unHyper . runGet get $ attrValue x of
            Left serializeError -> Left $ "Error deserializing: " <> serializeError
            Right value         -> Right value
          []  -> Left $ "No valid attribute, attributes list: " <> show attrList
          _   -> Left "More than one returned value"

putHyper :: HyperSerialize a => Client -> ByteString -> ByteString -> ByteString -> a -> QC.PropertyM IO (Either HyperclientReturnCode ())
putHyper client space key attribute value = do
    let serializedValue = runPut . put . Hyper $ value
    QC.run . join $ hyperPut client space key [Attribute attribute serializedValue (datatype value)]
    
getHyper :: HyperSerialize a => Client -> ByteString -> ByteString -> ByteString -> QC.PropertyM IO (Either String a)
getHyper client space key attribute = do
    eitherAttrList <- QC.run . join $ hyperGet client space key
    let retValue = getResult attribute eitherAttrList 
    case retValue of
      Left err -> QC.run $ do
        putStrLn $ "getHyper encountered error: " <> show err
        putStrLn $ "Attribute: "
        putStrLn $ show . fmap (filter (\x -> attrName x == attribute)) $ eitherAttrList
      _ -> return ()
    return $ retValue

propCanStore :: (Show a, Eq a, HyperSerialize a) => Client -> ByteString -> a 
                -> ByteString -> NonEmpty ByteString -> Property
propCanStore client attribute input space (NonEmpty key) =
  QC.monadicIO $ do
    r1 <- putHyper client space key attribute input
    eitherOutput <- getHyper client space key attribute
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
    client <- makeClient defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a floating point Double through HyperDex"
            $ \(value :: Double) -> propCanStore client "score" value defaultSpace
    return (test, closeClient client)

testCanStoreIntegers :: Test
testCanStoreIntegers = buildTestBracketed $ do
    client <- makeClient defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip an integer through HyperDex"
            $ \(value :: Int64) -> propCanStore client "profile_views" value defaultSpace
    return (test, closeClient client)

testCanStoreStrings :: Test
testCanStoreStrings = buildTestBracketed $ do
    client <- makeClient defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a string through HyperDex"
            $ \(value :: ByteString) -> propCanStore client "first" value defaultSpace
    return (test, closeClient client)

testCanStoreListOfStrings :: Test
testCanStoreListOfStrings = buildTestBracketed $ do
    client <- makeClient defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a list of strings through HyperDex"
            $ \(value :: [ByteString]) -> propCanStore client "pending_requests" value defaultSpace
    return (test, closeClient client)

testCanStoreListOfDoubles :: Test
testCanStoreListOfDoubles = buildTestBracketed $ do
    client <- makeClient defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a list of floating point doubles through HyperDex"
            $ \(value :: [Double]) -> propCanStore client "rankings" value defaultSpace
    return (test, closeClient client)

testCanStoreListOfIntegers :: Test
testCanStoreListOfIntegers = buildTestBracketed $ do
    client <- makeClient defaultHost defaultPort
    let test = 
          testProperty
            "Can round trip a list of integers through HyperDex"
            $ \(value :: [Int64]) -> propCanStore client "todolist" value defaultSpace
    return (test, closeClient client)

canCreateAndRemoveSpaces :: Test
canCreateAndRemoveSpaces = do
  testGroup "Can create and remove space" [ canCreateSpace, canRemoveSpace ]

internalTests :: Test
internalTests = mutuallyExclusive 
                $ plusTestOptions 
                  (mempty { topt_maximum_generated_tests = Just 1000
                          , topt_maximum_test_size = Just 64
                          })
                $ testGroup "Internal API Tests"
                  [ canCreateSpace
                  , testCanStoreIntegers
                  , testCanStoreStrings
                  , testCanStoreDoubles
                  , testCanStoreListOfStrings
                  , testCanStoreListOfDoubles
                  , testCanStoreListOfIntegers
                  , canRemoveSpace
                  ]