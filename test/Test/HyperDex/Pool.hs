{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}

module Test.HyperDex.Pool ( poolTests )
  where

import Test.HyperDex.Space

import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit hiding (Test)
import Control.Monad

import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck hiding (NonEmpty, (.&.))
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
import Data.Ratio
import Data.Monoid

import Data.Pool

import Data.Bits

testCanStoreLargeObject :: Pool Client -> Test
testCanStoreLargeObject clientPool = testCase "Can store a large object" $ do
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

getResult :: Text -> Either ReturnCode [Attribute] -> Either String Attribute
getResult _         (Left returnCode) = Left $ "Failure, returnCode: " <> show returnCode
getResult attribute (Right attrList)  =
  case (filter (\a -> attrName a == encodeUtf8 attribute) attrList) of
          [x] -> Right x
          []  -> Left $ "No valid attribute, attributes list: " <> show attrList
          _   -> Left "More than one returned value"

putHyper :: Pool Client -> Text -> ByteString -> Attribute -> QC.PropertyM IO (Either ReturnCode ())
putHyper clientPool space key attribute = do
    QC.run . join $ withResource clientPool $ \client -> putAsyncAttr client space key [attribute]

getHyper :: Pool Client -> Text -> ByteString -> Text -> QC.PropertyM IO (Either String Attribute)
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

propCanStore :: HyperSerialize a => Pool Client -> ByteString -> a 
                -> Text -> NonEmpty ByteString -> Property
propCanStore clientPool _ input space (NonEmpty key) =
  QC.monadicIO $ do
    let attributeName = decodeUtf8 $ pickAttributeName input
        attribute = mkAttributeUtf8 attributeName input
    _ <- putHyper clientPool space key attribute
    eitherOutput <- getHyper clientPool space key attributeName
    case eitherOutput of
      Right output -> do
        case attribute == output of
          True -> QC.assert True
          False -> do 
            QC.run $ do
              putStrLn $ "Failed to store value:"
              putStrLn $ "  space:  " <> show space
              putStrLn $ "  key:    " <> show key
              putStrLn $ "  attr:   " <> show attribute
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

propCanConditionalPutNumeric :: Pool Client -> Text
                             -> HyperRelated -> NonEmpty ByteString -> Property
propCanConditionalPutNumeric
  clientPool space
  (HyperRelated ( MkHyperSerializable initial
                , MkHyperSerializable failing
                , MkHyperSerializable succeeding
                , predicate))
  (NonEmpty key) =
    QC.monadicIO $ do
      let attributeName = decodeUtf8 $ pickAttributeName initial
          initialAttribute = mkAttributeUtf8 attributeName initial
          failingAttribute = mkAttributeUtf8 attributeName failing
          failingAttributeCheck = mkAttributeCheckUtf8 attributeName failing predicate
          succeedingAttribute = mkAttributeUtf8 attributeName succeeding
          succeedingAttributeCheck = mkAttributeCheckUtf8 attributeName succeeding predicate
      _ <- QC.run . join $ withResource clientPool $
             \client -> putAsyncAttr client space key [initialAttribute]
      failingResult <- QC.run . join $ withResource clientPool $
                          \client -> putConditionalAsyncAttr client space key [failingAttributeCheck] [failingAttribute]
      case failingResult of
        Left HyperclientCmpfail -> return ()
        _ -> do
          QC.run $ do
            putStrLn $ "Conditional store with failing put (not failing correctly):"
            putStrLn $ "  space:  " <> show space
            putStrLn $ "  key:    " <> show key
            putStrLn $ "  attr:   " <> show initialAttribute
            putStrLn $ "  attr:   " <> show failingAttribute
            putStrLn $ "  attr:   " <> show succeedingAttribute
            putStrLn $ "  output: " <> show failingResult
          QC.assert False
      _ <- QC.run . join $ withResource clientPool $
             \client -> putConditionalAsyncAttr client space key [succeedingAttributeCheck] [succeedingAttribute]
      eitherOutput <- getHyper clientPool space key attributeName
      case eitherOutput of
        Right output -> do
          case succeedingAttribute == output of
            True -> QC.assert True
            False -> do 
              QC.run $ do
                putStrLn $ "Failed to store value:"
                putStrLn $ "  space:  " <> show space
                putStrLn $ "  key:    " <> show key
                putStrLn $ "  attr:   " <> show initialAttribute
                putStrLn $ "  attr:   " <> show failingAttribute
                putStrLn $ "  attr:   " <> show succeedingAttribute
                putStrLn $ "  output: " <> show output
              QC.assert False
        Left reason  -> do 
          QC.run $ do
            putStrLn $ "Failed to retrieve value:"
            putStrLn $ "  space:  " <> show space
            putStrLn $ "  key:    " <> show key
            putStrLn $ "  attr:   " <> show initialAttribute
            putStrLn $ "  attr:   " <> show failingAttribute
            putStrLn $ "  attr:   " <> show succeedingAttribute
            putStrLn $ "  reason: " <> show reason
          QC.assert False

propCanAtomicOpIntegral :: Pool Client
                        -> Text
                        -> Int64
                        -> Int64
                        -> NumericAtomicOp
                        -> NonEmpty ByteString
                        -> Property
propCanAtomicOpIntegral clientPool space _ _ _ (NonEmpty key) =
  QC.monadicIO $ do
    let initial = 9223372036854775807
        operand = 9223372036854775807
        operator = AtomicAdd
    let attributeName = decodeUtf8 $ pickAttributeName initial
        attribute   = mkAttributeUtf8 attributeName initial
        opAttribute = mkAttributeUtf8 attributeName operand
    _ <- QC.run . join $ withResource clientPool $ \client -> putAsyncAttr client space key [attribute]
    let hyperCall =
          case operator of
            AtomicAdd -> putAtomicAdd 
            AtomicSub -> putAtomicSub 
            AtomicMul -> putAtomicMul 
            AtomicDiv -> putAtomicDiv 
            AtomicMod -> putAtomicMod 
            AtomicAnd -> putAtomicAnd 
            AtomicOr  -> putAtomicOr  
            AtomicXor -> putAtomicXor
        localOp :: Int64 -> Int64 -> Int64 
        localOp =
          case operator of
            AtomicAdd -> (+) 
            AtomicSub -> (-) 
            AtomicMul -> (*) 
            AtomicDiv -> div 
            AtomicMod -> mod 
            AtomicAnd -> (.&.) 
            AtomicOr  -> (.|.)  
            AtomicXor -> xor
    atomicOpResult <- QC.run . join $ withResource clientPool $ \client -> hyperCall client space key [opAttribute]
    case atomicOpResult of
      Left HyperclientOverflow -> do
        -- Acceptable overflows (Hyperdex 1.04.rc)
        case (operator, signum initial == signum (initial `localOp` operand)) of
          (AtomicAdd, False) -> QC.assert True
          (AtomicMul, False) -> QC.assert True
          _ -> do
            QC.run $ do
              putStrLn $ "Failed in running atomic op:"
              putStrLn $ "  space:    " <> show space
              putStrLn $ "  key:      " <> show key
              putStrLn $ "  attr:     " <> show attribute
              putStrLn $ "  initial:  " <> show initial
              putStrLn $ "  operator: " <> show operator
              putStrLn $ "  operand:  " <> show operand
              putStrLn $ "  expected: " <> show (initial `localOp` operand)
              putStrLn $ "  result:   " <> show atomicOpResult
            QC.assert False
      Left err -> do
        QC.run $ do
          putStrLn $ "Failed in running atomic op:"
          putStrLn $ "  space:    " <> show space
          putStrLn $ "  key:      " <> show key
          putStrLn $ "  attr:     " <> show attribute
          putStrLn $ "  initial:  " <> show initial
          putStrLn $ "  operator: " <> show operator
          putStrLn $ "  operand:  " <> show operand
          putStrLn $ "  expected: " <> show (initial `localOp` operand)
          putStrLn $ "  error:    " <> show err
        QC.assert False
      Right () -> do
        eitherOutput <- getHyper clientPool space key attributeName
        case (eitherOutput >>= deserialize . attrValue) :: Either String Int64 of
          Right output -> do
            case output == (initial `localOp` operand)  of
              True -> QC.assert True
              False -> do 
                QC.run $ do
                  putStrLn $ "Failed to store value:"
                  putStrLn $ "  space:    " <> show space
                  putStrLn $ "  key:      " <> show key
                  putStrLn $ "  attr:     " <> show attribute
                  putStrLn $ "  initial:  " <> show initial
                  putStrLn $ "  operator: " <> show operator
                  putStrLn $ "  operand:  " <> show operand
                  putStrLn $ "  output:   " <> show output
                  putStrLn $ "  expected: " <> show (initial `localOp` operand)
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
  connect defaultConnectInfo

closeAction client = do
  close client

mkPool = createPool createAction closeAction 4 (fromRational $ 1%2) 10

testCanRoundtrip :: Pool Client -> Test
testCanRoundtrip clientPool =
  testProperty
    "roundtrip-pooled"
    $ \(MkHyperSerializable value) -> propCanStore clientPool "arbitrary" value defaultSpace

testConditional :: Pool Client -> Test
testConditional clientPool =
  testProperty
    "conditional"
    $ propCanConditionalPutNumeric clientPool defaultSpace

testAtomicIntegral :: Pool Client -> Test
testAtomicIntegral clientPool =
  testProperty
    "atomic-integral"
    $ propCanAtomicOpIntegral clientPool defaultSpace


poolTests :: Test
poolTests = buildTest $ do
  clientPool <- mkPool 
  let tests = mutuallyExclusive
              $ testGroup "pooled"
              $ fmap (\f -> f clientPool)
                [ testCanStoreLargeObject
                , testCanRoundtrip
                , testConditional
                , testAtomicIntegral
                ]
  return tests
