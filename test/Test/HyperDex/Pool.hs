{-# LANGUAGE OverloadedStrings, ScopedTypeVariables, FlexibleContexts #-}

module Test.HyperDex.Pool ( poolTests )
  where

import Test.HyperDex.Space

import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit hiding (Test)
import Control.Monad

import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck hiding ((.&.))
import qualified Test.QuickCheck.Monadic as QC

import Test.HyperDex.Util

import Data.Text (Text)
import Data.Text.Encoding
import Data.ByteString.Char8 (ByteString, append)

import Database.HyperDex
import Database.HyperDex.Utf8

import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)
import qualified Data.Map as Map

import Data.Int
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
                -> Text -> NonEmptyBS ByteString -> Property
propCanStore clientPool _ input space (NonEmptyBS key) =
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
                             -> HyperRelated -> NonEmptyBS ByteString -> Property
propCanConditionalPutNumeric
  clientPool space
  (HyperRelated ( MkHyperSerializable initial
                , MkHyperSerializable failing
                , MkHyperSerializable succeeding
                , predicate))
  (NonEmptyBS key) =
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

type AsyncOp = Client -> Text -> ByteString -> [Attribute] -> AsyncResult ()

generateTestPropAtomicOp :: (Show a, Eq a, HyperSerialize a,
                             Show b, Eq b, HyperSerialize b, 
                             Show x, Arbitrary x)
                         => String         -- ^ The test name
                         -> AsyncOp        -- ^ The HyperDex operation to be performed
                         -> (a -> b -> a)  -- ^ The operation used to simulate execution 
                         -> (x -> (a, b))  -- ^ The deconstructor for the arbitrary type
                         -> (Pool Client -> Text -> Test)
generateTestPropAtomicOp testName hyperCall localOp decons =
  \clientPool space -> testProperty testName $
    \(NonEmptyBS key) arbitraryInput ->
      QC.monadicIO $ do
      let (initial, operand) = decons arbitraryInput
          attributeName      = decodeUtf8 $ pickAttributeName initial
          attribute          = mkAttributeUtf8 attributeName initial
          opAttribute        = mkAttributeUtf8 attributeName operand
      _ <- QC.run . join $ withResource clientPool $ \client -> putAsyncAttr client space key [attribute]
      atomicOpResult <- QC.run . join $ withResource clientPool $ \client -> hyperCall client space key [opAttribute]
      case atomicOpResult of
        Left err -> do
          QC.run $ do
            putStrLn $ "Failed in running atomic op:"
            putStrLn $ "  test name: " <> show testName
            putStrLn $ "  space:     " <> show space
            putStrLn $ "  key:       " <> show key
            putStrLn $ "  attr:      " <> show attribute
            putStrLn $ "  initial:   " <> show initial
            putStrLn $ "  operand:   " <> show operand
            putStrLn $ "  expected:  " <> show (initial `localOp` operand)
            putStrLn $ "  error:     " <> show err
          QC.assert False
        Right () -> do
          eitherOutput <- getHyper clientPool space key attributeName
          case eitherOutput >>= deserialize . attrValue of
            Right output -> do
              case output == (initial `localOp` operand)  of
                True -> QC.assert True
                False -> do 
                  QC.run $ do
                    putStrLn $ "Failed to store value:"
                    putStrLn $ "  test name: " <> show testName
                    putStrLn $ "  space:     " <> show space
                    putStrLn $ "  key:       " <> show key
                    putStrLn $ "  attr:      " <> show attribute
                    putStrLn $ "  initial:   " <> show initial
                    putStrLn $ "  operand:   " <> show operand
                    putStrLn $ "  output:    " <> show output
                    putStrLn $ "  expected:  " <> show (initial `localOp` operand)
                  QC.assert False
            Left reason  -> do 
              QC.run $ do
                putStrLn $ "Failed to retrieve value:"
                putStrLn $ "  test name: " <> show testName
                putStrLn $ "  space:  " <> show space
                putStrLn $ "  key:    " <> show key
                putStrLn $ "  attr:   " <> show attribute
                putStrLn $ "  reason: " <> show reason
              QC.assert False

type AsyncMapOp = Client -> Text -> ByteString -> [MapAttribute] -> AsyncResult ()

generateTestPropAtomicMapOp :: (Show k, Show v, Eq k, Eq v, HyperSerialize k, HyperSerialize v, 
                                HyperSerialize (Map k v), 
                                Show x, Arbitrary x)
                            => String         -- ^ The test name
                            -> AsyncMapOp     -- ^ The HyperDex operation to be performed
                            -> (Map k v -> Map k v -> Map k v)  -- ^ The operation used to simulate execution 
                            -> (x -> (Map k v, Map k v))  -- ^ The deconstructor for the arbitrary type
                            -> (Pool Client -> Text -> Test)
generateTestPropAtomicMapOp testName hyperCall localOp decons =
  \clientPool space -> testProperty testName $
    \(NonEmptyBS key) arbitraryInput ->
      QC.monadicIO $ do
      let (initial, operand) = decons arbitraryInput
          attributeName      = decodeUtf8 $ pickAttributeName initial
          attribute          = mkAttributeUtf8 attributeName initial
          opAttribute        = mkMapAttributesFromMapUtf8 attributeName operand
      _ <- QC.run . join $ withResource clientPool $ \client -> putAsyncAttr client space key [attribute]
      atomicOpResult <- QC.run . join $ withResource clientPool $ \client -> hyperCall client space key opAttribute
      case atomicOpResult of
        Left err -> do
          QC.run $ do
            putStrLn $ "Failed in running atomic op:"
            putStrLn $ "  test name: " <> show testName
            putStrLn $ "  space:     " <> show space
            putStrLn $ "  key:       " <> show key
            putStrLn $ "  attr:      " <> show attribute
            putStrLn $ "  initial:   " <> show initial
            putStrLn $ "  operand:   " <> show operand
            putStrLn $ "  expected:  " <> show (initial `localOp` operand)
            putStrLn $ "  error:     " <> show err
          QC.assert False
        Right () -> do
          eitherOutput <- getHyper clientPool space key attributeName
          case eitherOutput >>= deserialize . attrValue of
            Right output -> do
              case output == (initial `localOp` operand)  of
                True -> QC.assert True
                False -> do 
                  QC.run $ do
                    putStrLn $ "Failed to store value:"
                    putStrLn $ "  test name: " <> show testName
                    putStrLn $ "  space:     " <> show space
                    putStrLn $ "  key:       " <> show key
                    putStrLn $ "  attr:      " <> show attribute
                    putStrLn $ "  initial:   " <> show initial
                    putStrLn $ "  operand:   " <> show operand
                    putStrLn $ "  output:    " <> show output
                    putStrLn $ "  expected:  " <> show (initial `localOp` operand)
                  QC.assert False
            Left reason  -> do 
              QC.run $ do
                putStrLn $ "Failed to retrieve value:"
                putStrLn $ "  test name: " <> show testName
                putStrLn $ "  space:  " <> show space
                putStrLn $ "  key:    " <> show key
                putStrLn $ "  attr:   " <> show attribute
                putStrLn $ "  reason: " <> show reason
              QC.assert False

testAtomic :: Pool Client -> Text -> Test
testAtomic clientPool space =
  testGroup "atomic"
  $ fmap (\f -> f clientPool space)
    [ testAtomicInteger
    , testAtomicFloat
    , testAtomicString
    , testAtomicList
    , testAtomicSet
    , testAtomicMap
    ]

type Op2 a b = a -> b -> a
type Op a    = Op2 a a

testAtomicInteger :: Pool Client -> Text -> Test
testAtomicInteger clientPool space =
  testGroup "integer"
  $ fmap (\f -> f clientPool space)
    [ generateTestPropAtomicOp "add" putAtomicAdd (  (+) :: Op Int64) safeAddInt64
    , generateTestPropAtomicOp "sub" putAtomicSub (  (-) :: Op Int64) safeSubtractInt64
    , generateTestPropAtomicOp "mul" putAtomicMul (  (*) :: Op Int64) safeMultiplyInt64
    , generateTestPropAtomicOp "div" putAtomicDiv (  div :: Op Int64) safeDivideInt64
    , generateTestPropAtomicOp "mod" putAtomicMod (  mod :: Op Int64) safeDivideInt64
    , generateTestPropAtomicOp "and" putAtomicAnd ((.&.) :: Op Int64) id
    , generateTestPropAtomicOp "or"  putAtomicOr  ((.|.) :: Op Int64) id
    , generateTestPropAtomicOp "xor" putAtomicXor (  xor :: Op Int64) id
    ]

testAtomicFloat :: Pool Client -> Text -> Test
testAtomicFloat clientPool space =
  testGroup "float"
  $ fmap (\f -> f clientPool space)
    [ generateTestPropAtomicOp "add" putAtomicAdd ((+) :: Op Double) id
    , generateTestPropAtomicOp "sub" putAtomicSub ((-) :: Op Double) id
    , generateTestPropAtomicOp "mul" putAtomicMul ((*) :: Op Double) id
    , generateTestPropAtomicOp "div" putAtomicDiv ((/) :: Op Double) safeDivideDouble
    ]

testAtomicString :: Pool Client -> Text -> Test
testAtomicString clientPool space =
  testGroup "string"
  $ fmap (\f -> f clientPool space)
    [ generateTestPropAtomicOp "prepend" putAtomicPrepend prepend id
    , generateTestPropAtomicOp "append"  putAtomicAppend  append  id
    ]
  where prepend = flip append

testAtomicList :: Pool Client -> Text -> Test
testAtomicList clientPool space =
  testGroup "list"
  [ testGroup "int"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "lpush" putAtomicListLPush (prepend :: Op2 [Int64] Int64) id
        , generateTestPropAtomicOp "rpush" putAtomicListRPush (append  :: Op2 [Int64] Int64) id
        ]
  , testGroup "float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "lpush" putAtomicListLPush (prepend :: Op2 [Double] Double) id
        , generateTestPropAtomicOp "rpush" putAtomicListRPush (append  :: Op2 [Double] Double) id
        ]
  , testGroup "string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "lpush" putAtomicListLPush (prepend :: Op2 [ByteString] ByteString) id
        , generateTestPropAtomicOp "rpush" putAtomicListRPush (append  :: Op2 [ByteString] ByteString) id
        ]
  ]
  where append, prepend :: Op2 [a] a
        append  a b = a ++ [b]
        prepend a b = [b] ++ a

testAtomicSet :: Pool Client -> Text -> Test
testAtomicSet clientPool space =
  testGroup "set"
  [ testGroup "int"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "add"       putAtomicSetAdd        (flip Set.insert  :: Op2 (Set Int64) Int64) id
        , generateTestPropAtomicOp "remove"    putAtomicSetRemove     (flip Set.delete  :: Op2 (Set Int64) Int64) id
        , generateTestPropAtomicOp "intersect" putAtomicSetIntersect  (Set.intersection :: Op (Set Int64)) id
        , generateTestPropAtomicOp "union"     putAtomicSetUnion      (Set.union        :: Op (Set Int64)) id
        ]
  , testGroup "float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "add"       putAtomicSetAdd        (flip Set.insert  :: Op2 (Set Double) Double) id
        , generateTestPropAtomicOp "remove"    putAtomicSetRemove     (flip Set.delete  :: Op2 (Set Double) Double) id
        , generateTestPropAtomicOp "intersect" putAtomicSetIntersect  (Set.intersection :: Op (Set Double)) id
        , generateTestPropAtomicOp "union"     putAtomicSetUnion      (Set.union        :: Op (Set Double)) id
        ]
  , testGroup "string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "add"       putAtomicSetAdd        (flip Set.insert  :: Op2 (Set ByteString) ByteString) id
        , generateTestPropAtomicOp "remove"    putAtomicSetRemove     (flip Set.delete  :: Op2 (Set ByteString) ByteString) id
        , generateTestPropAtomicOp "intersect" putAtomicSetIntersect  (Set.intersection :: Op (Set ByteString)) id
        , generateTestPropAtomicOp "union"     putAtomicSetUnion      (Set.union        :: Op (Set ByteString)) id
        ]
  ]

opOverMap :: Ord k => (a -> b -> a) -> Map k a -> Map k b -> Map k a
opOverMap operator initials operands = Map.mapWithKey go initials
  where
    go k a = case k `Map.lookup` operands of
                Just b  -> a `operator` b
                Nothing -> a

testAtomicMap :: Pool Client -> Text -> Test
testAtomicMap clientPool space =
  testGroup "map"
  [ testGroup "int-int"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" putAtomicMapInsert (Map.union       :: Op (Map Int64 Int64)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" putAtomicMapDelete (Map.difference  :: Op (Map Int64 Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    putAtomicMapAdd    (opOverMap (+)   :: Op (Map Int64 Int64)) (checkAddition . overlappingMaps)
        , generateTestPropAtomicMapOp "sub"    putAtomicMapSub    (opOverMap (-)   :: Op (Map Int64 Int64)) (checkSubtraction . overlappingMaps)
        , generateTestPropAtomicMapOp "mul"    putAtomicMapMul    (opOverMap (*)   :: Op (Map Int64 Int64)) (checkMultiplication . overlappingMaps)
        , generateTestPropAtomicMapOp "div"    putAtomicMapDiv    (opOverMap div   :: Op (Map Int64 Int64)) (checkIntDivision . overlappingMaps)
        , generateTestPropAtomicMapOp "and"    putAtomicMapAnd    (opOverMap (.&.) :: Op (Map Int64 Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "or"     putAtomicMapOr     (opOverMap (.|.) :: Op (Map Int64 Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "xor"    putAtomicMapXor    (opOverMap xor   :: Op (Map Int64 Int64)) overlappingMaps
        ]
  , testGroup "int-float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" putAtomicMapInsert (Map.union      :: Op (Map Int64 Double)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" putAtomicMapDelete (Map.difference :: Op (Map Int64 Double)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    putAtomicMapAdd    (opOverMap (+)   :: Op (Map Int64 Double)) overlappingMaps
        , generateTestPropAtomicMapOp "sub"    putAtomicMapSub    (opOverMap (-)   :: Op (Map Int64 Double)) overlappingMaps
        , generateTestPropAtomicMapOp "mul"    putAtomicMapMul    (opOverMap (*)   :: Op (Map Int64 Double)) overlappingMaps
        , generateTestPropAtomicMapOp "div"    putAtomicMapDiv    (opOverMap (/)   :: Op (Map Int64 Double)) (checkDoubleDivision . overlappingMaps)
        ]
  , testGroup "int-string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" putAtomicMapInsert (Map.union      :: Op (Map Int64 ByteString)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" putAtomicMapDelete (Map.difference :: Op (Map Int64 ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" putAtomicMapStringPrepend (opOverMap prepend :: Op (Map Int64 ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" putAtomicMapStringAppend  (opOverMap append  :: Op (Map Int64 ByteString)) overlappingMaps
        ]
  ,  testGroup "float-int"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" putAtomicMapInsert (Map.union       :: Op (Map Double Int64)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" putAtomicMapDelete (Map.difference  :: Op (Map Double Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    putAtomicMapAdd    (opOverMap (+)   :: Op (Map Double Int64)) (checkAddition . overlappingMaps)
        , generateTestPropAtomicMapOp "sub"    putAtomicMapSub    (opOverMap (-)   :: Op (Map Double Int64)) (checkSubtraction . overlappingMaps)
        , generateTestPropAtomicMapOp "mul"    putAtomicMapMul    (opOverMap (*)   :: Op (Map Double Int64)) (checkMultiplication . overlappingMaps)
        , generateTestPropAtomicMapOp "div"    putAtomicMapDiv    (opOverMap div   :: Op (Map Double Int64)) (checkIntDivision . overlappingMaps)
        , generateTestPropAtomicMapOp "and"    putAtomicMapAnd    (opOverMap (.&.) :: Op (Map Double Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "or"     putAtomicMapOr     (opOverMap (.|.) :: Op (Map Double Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "xor"    putAtomicMapXor    (opOverMap xor   :: Op (Map Double Int64)) overlappingMaps
        ]
  , testGroup "float-float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" putAtomicMapInsert (Map.union      :: Op (Map Double Double)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" putAtomicMapDelete (Map.difference :: Op (Map Double Double)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    putAtomicMapAdd    (opOverMap (+)   :: Op (Map Double Double)) overlappingMaps
        , generateTestPropAtomicMapOp "sub"    putAtomicMapSub    (opOverMap (-)   :: Op (Map Double Double)) overlappingMaps
        , generateTestPropAtomicMapOp "mul"    putAtomicMapMul    (opOverMap (*)   :: Op (Map Double Double)) overlappingMaps
        , generateTestPropAtomicMapOp "div"    putAtomicMapDiv    (opOverMap (/)   :: Op (Map Double Double)) (checkDoubleDivision . overlappingMaps)
        ]
  , testGroup "float-string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" putAtomicMapInsert (Map.union      :: Op (Map Double ByteString)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" putAtomicMapDelete (Map.difference :: Op (Map Double ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" putAtomicMapStringPrepend (opOverMap prepend :: Op (Map Double ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" putAtomicMapStringAppend  (opOverMap append  :: Op (Map Double ByteString)) overlappingMaps
        ]
  ,  testGroup "string-int"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" putAtomicMapInsert (Map.union       :: Op (Map ByteString Int64)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" putAtomicMapDelete (Map.difference  :: Op (Map ByteString Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    putAtomicMapAdd    (opOverMap (+)   :: Op (Map ByteString Int64)) (checkAddition . overlappingMaps)
        , generateTestPropAtomicMapOp "sub"    putAtomicMapSub    (opOverMap (-)   :: Op (Map ByteString Int64)) (checkSubtraction . overlappingMaps)
        , generateTestPropAtomicMapOp "mul"    putAtomicMapMul    (opOverMap (*)   :: Op (Map ByteString Int64)) (checkMultiplication . overlappingMaps)
        , generateTestPropAtomicMapOp "div"    putAtomicMapDiv    (opOverMap div   :: Op (Map ByteString Int64)) (checkIntDivision . overlappingMaps)
        , generateTestPropAtomicMapOp "and"    putAtomicMapAnd    (opOverMap (.&.) :: Op (Map ByteString Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "or"     putAtomicMapOr     (opOverMap (.|.) :: Op (Map ByteString Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "xor"    putAtomicMapXor    (opOverMap xor   :: Op (Map ByteString Int64)) overlappingMaps
        ]
  , testGroup "string-float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" putAtomicMapInsert (Map.union      :: Op (Map ByteString Double)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" putAtomicMapDelete (Map.difference :: Op (Map ByteString Double)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    putAtomicMapAdd    (opOverMap (+)   :: Op (Map ByteString Double)) overlappingMaps
        , generateTestPropAtomicMapOp "sub"    putAtomicMapSub    (opOverMap (-)   :: Op (Map ByteString Double)) overlappingMaps
        , generateTestPropAtomicMapOp "mul"    putAtomicMapMul    (opOverMap (*)   :: Op (Map ByteString Double)) overlappingMaps
        , generateTestPropAtomicMapOp "div"    putAtomicMapDiv    (opOverMap (/)   :: Op (Map ByteString Double)) (checkDoubleDivision . overlappingMaps)
        ]
  , testGroup "string-string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" putAtomicMapInsert (Map.union      :: Op (Map ByteString ByteString)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" putAtomicMapDelete (Map.difference :: Op (Map ByteString ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" putAtomicMapStringPrepend (opOverMap prepend :: Op (Map ByteString ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" putAtomicMapStringAppend  (opOverMap append  :: Op (Map ByteString ByteString)) overlappingMaps
        ]
  ]
  where prepend = flip append

createAction = do
  connect defaultConnectInfo

closeAction client = do
  close client

mkPool = createPool createAction closeAction 4 0.5 10

testCanRoundtrip :: Pool Client -> Test
testCanRoundtrip clientPool =
  testProperty
    "roundtrip"
    $ \(MkHyperSerializable value) -> propCanStore clientPool "arbitrary" value defaultSpace

testConditional :: Pool Client -> Test
testConditional clientPool =
  testProperty
    "conditional"
    $ propCanConditionalPutNumeric clientPool defaultSpace

testSearch :: Pool Client -> Text -> Test
testSearch clientPool space = testCase "atomic/search" $ do
  withResource clientPool $ \client -> do
    searchResultStart <- search client (encodeUtf8 space) []
    searchResult <- searchResultStart
    go searchResult
    return ()
  where
    go (Left e) = do
      putStrLn $ "Ended with " ++ (show e)
    go (Right (SearchStream (a, next))) = do
      putStrLn $ "Found item:\n  " ++ (show a)
      nextItem <- next
      go nextItem

poolTests :: Test
poolTests = buildTest $ do
  clientPool <- mkPool 
  let tests = mutuallyExclusive
              $ testGroup "pooled"
              $ fmap (\f -> f clientPool)
                [ testCanStoreLargeObject
                , testCanRoundtrip
                , testConditional
                ]
                ++
                fmap (\f -> f clientPool defaultSpace)
                [ testAtomic
                , testSearch
                ]
  return tests
