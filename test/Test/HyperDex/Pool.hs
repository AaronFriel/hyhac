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

import Control.Concurrent (threadDelay)

import Control.Applicative

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
  result <- join $ withResource clientPool $ \client -> put client defaultSpace "large" attrs
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
    QC.run . join $ withResource clientPool $ \client -> put client space key [attribute]

getHyper :: Pool Client -> Text -> ByteString -> Text -> QC.PropertyM IO (Either String Attribute)
getHyper clientPool space key attribute = do
    eitherAttrList <- QC.run . join $ withResource clientPool $ \client -> get client space key
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
             \client -> put client space key [initialAttribute]
      failingResult <- QC.run . join $ withResource clientPool $
                          \client -> putConditional client space key [failingAttributeCheck] [failingAttribute]
      case failingResult of
        Left HyperdexClientCmpfail -> return ()
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
             \client -> putConditional client space key [succeedingAttributeCheck] [succeedingAttribute]
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
      _ <- QC.run . join $ withResource clientPool $ \client -> put client space key [attribute]
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
      _ <- QC.run . join $ withResource clientPool $ \client -> put client space key [attribute]
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
    [ generateTestPropAtomicOp "add" atomicAdd (  (+) :: Op Int64) safeAddInt64
    , generateTestPropAtomicOp "sub" atomicSub (  (-) :: Op Int64) safeSubtractInt64
    , generateTestPropAtomicOp "mul" atomicMul (  (*) :: Op Int64) safeMultiplyInt64
    , generateTestPropAtomicOp "div" atomicDiv (  div :: Op Int64) safeDivideInt64
    , generateTestPropAtomicOp "mod" atomicMod (  mod :: Op Int64) safeDivideInt64
    , generateTestPropAtomicOp "and" atomicAnd ((.&.) :: Op Int64) id
    , generateTestPropAtomicOp "or"  atomicOr  ((.|.) :: Op Int64) id
    , generateTestPropAtomicOp "xor" atomicXor (  xor :: Op Int64) id
    ]

testAtomicFloat :: Pool Client -> Text -> Test
testAtomicFloat clientPool space =
  testGroup "float"
  $ fmap (\f -> f clientPool space)
    [ generateTestPropAtomicOp "add" atomicAdd ((+) :: Op Double) id
    , generateTestPropAtomicOp "sub" atomicSub ((-) :: Op Double) id
    , generateTestPropAtomicOp "mul" atomicMul ((*) :: Op Double) id
    , generateTestPropAtomicOp "div" atomicDiv ((/) :: Op Double) safeDivideDouble
    ]

testAtomicString :: Pool Client -> Text -> Test
testAtomicString clientPool space =
  testGroup "string"
  $ fmap (\f -> f clientPool space)
    [ generateTestPropAtomicOp "prepend" atomicStringPrepend prepend id
    , generateTestPropAtomicOp "append"  atomicStringAppend  append  id
    ]
  where prepend = flip append

testAtomicList :: Pool Client -> Text -> Test
testAtomicList clientPool space =
  testGroup "list"
  [ testGroup "int"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "lpush" atomicListLPush (prepend :: Op2 [Int64] Int64) id
        , generateTestPropAtomicOp "rpush" atomicListRPush (append  :: Op2 [Int64] Int64) id
        ]
  , testGroup "float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "lpush" atomicListLPush (prepend :: Op2 [Double] Double) id
        , generateTestPropAtomicOp "rpush" atomicListRPush (append  :: Op2 [Double] Double) id
        ]
  , testGroup "string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "lpush" atomicListLPush (prepend :: Op2 [ByteString] ByteString) id
        , generateTestPropAtomicOp "rpush" atomicListRPush (append  :: Op2 [ByteString] ByteString) id
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
        [ generateTestPropAtomicOp "add"       atomicSetAdd        (flip Set.insert  :: Op2 (Set Int64) Int64) id
        , generateTestPropAtomicOp "remove"    atomicSetRemove     (flip Set.delete  :: Op2 (Set Int64) Int64) id
        , generateTestPropAtomicOp "intersect" atomicSetIntersect  (Set.intersection :: Op (Set Int64)) id
        , generateTestPropAtomicOp "union"     atomicSetUnion      (Set.union        :: Op (Set Int64)) id
        ]
  , testGroup "float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "add"       atomicSetAdd        (flip Set.insert  :: Op2 (Set Double) Double) id
        , generateTestPropAtomicOp "remove"    atomicSetRemove     (flip Set.delete  :: Op2 (Set Double) Double) id
        , generateTestPropAtomicOp "intersect" atomicSetIntersect  (Set.intersection :: Op (Set Double)) id
        , generateTestPropAtomicOp "union"     atomicSetUnion      (Set.union        :: Op (Set Double)) id
        ]
  , testGroup "string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "add"       atomicSetAdd        (flip Set.insert  :: Op2 (Set ByteString) ByteString) id
        , generateTestPropAtomicOp "remove"    atomicSetRemove     (flip Set.delete  :: Op2 (Set ByteString) ByteString) id
        , generateTestPropAtomicOp "intersect" atomicSetIntersect  (Set.intersection :: Op (Set ByteString)) id
        , generateTestPropAtomicOp "union"     atomicSetUnion      (Set.union        :: Op (Set ByteString)) id
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
        [ generateTestPropAtomicMapOp "insert" atomicMapInsert (Map.union       :: Op (Map Int64 Int64)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" atomicMapDelete (Map.difference  :: Op (Map Int64 Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    atomicMapAdd    (opOverMap (+)   :: Op (Map Int64 Int64)) (checkAddition . overlappingMaps)
        , generateTestPropAtomicMapOp "sub"    atomicMapSub    (opOverMap (-)   :: Op (Map Int64 Int64)) (checkSubtraction . overlappingMaps)
        , generateTestPropAtomicMapOp "mul"    atomicMapMul    (opOverMap (*)   :: Op (Map Int64 Int64)) (checkMultiplication . overlappingMaps)
        , generateTestPropAtomicMapOp "div"    atomicMapDiv    (opOverMap div   :: Op (Map Int64 Int64)) (checkIntDivision . overlappingMaps)
        , generateTestPropAtomicMapOp "and"    atomicMapAnd    (opOverMap (.&.) :: Op (Map Int64 Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "or"     atomicMapOr     (opOverMap (.|.) :: Op (Map Int64 Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "xor"    atomicMapXor    (opOverMap xor   :: Op (Map Int64 Int64)) overlappingMaps
        ]
  , testGroup "int-float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" atomicMapInsert (Map.union      :: Op (Map Int64 Double)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" atomicMapDelete (Map.difference :: Op (Map Int64 Double)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    atomicMapAdd    (opOverMap (+)   :: Op (Map Int64 Double)) overlappingMaps
        , generateTestPropAtomicMapOp "sub"    atomicMapSub    (opOverMap (-)   :: Op (Map Int64 Double)) overlappingMaps
        , generateTestPropAtomicMapOp "mul"    atomicMapMul    (opOverMap (*)   :: Op (Map Int64 Double)) overlappingMaps
        , generateTestPropAtomicMapOp "div"    atomicMapDiv    (opOverMap (/)   :: Op (Map Int64 Double)) (checkDoubleDivision . overlappingMaps)
        ]
  , testGroup "int-string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" atomicMapInsert (Map.union      :: Op (Map Int64 ByteString)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" atomicMapDelete (Map.difference :: Op (Map Int64 ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" atomicMapStringPrepend (opOverMap prepend :: Op (Map Int64 ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" atomicMapStringAppend  (opOverMap append  :: Op (Map Int64 ByteString)) overlappingMaps
        ]
  ,  testGroup "float-int"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" atomicMapInsert (Map.union       :: Op (Map Double Int64)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" atomicMapDelete (Map.difference  :: Op (Map Double Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    atomicMapAdd    (opOverMap (+)   :: Op (Map Double Int64)) (checkAddition . overlappingMaps)
        , generateTestPropAtomicMapOp "sub"    atomicMapSub    (opOverMap (-)   :: Op (Map Double Int64)) (checkSubtraction . overlappingMaps)
        , generateTestPropAtomicMapOp "mul"    atomicMapMul    (opOverMap (*)   :: Op (Map Double Int64)) (checkMultiplication . overlappingMaps)
        , generateTestPropAtomicMapOp "div"    atomicMapDiv    (opOverMap div   :: Op (Map Double Int64)) (checkIntDivision . overlappingMaps)
        , generateTestPropAtomicMapOp "and"    atomicMapAnd    (opOverMap (.&.) :: Op (Map Double Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "or"     atomicMapOr     (opOverMap (.|.) :: Op (Map Double Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "xor"    atomicMapXor    (opOverMap xor   :: Op (Map Double Int64)) overlappingMaps
        ]
  , testGroup "float-float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" atomicMapInsert (Map.union      :: Op (Map Double Double)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" atomicMapDelete (Map.difference :: Op (Map Double Double)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    atomicMapAdd    (opOverMap (+)   :: Op (Map Double Double)) overlappingMaps
        , generateTestPropAtomicMapOp "sub"    atomicMapSub    (opOverMap (-)   :: Op (Map Double Double)) overlappingMaps
        , generateTestPropAtomicMapOp "mul"    atomicMapMul    (opOverMap (*)   :: Op (Map Double Double)) overlappingMaps
        , generateTestPropAtomicMapOp "div"    atomicMapDiv    (opOverMap (/)   :: Op (Map Double Double)) (checkDoubleDivision . overlappingMaps)
        ]
  , testGroup "float-string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" atomicMapInsert (Map.union      :: Op (Map Double ByteString)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" atomicMapDelete (Map.difference :: Op (Map Double ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" atomicMapStringPrepend (opOverMap prepend :: Op (Map Double ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" atomicMapStringAppend  (opOverMap append  :: Op (Map Double ByteString)) overlappingMaps
        ]
  ,  testGroup "string-int"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" atomicMapInsert (Map.union       :: Op (Map ByteString Int64)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" atomicMapDelete (Map.difference  :: Op (Map ByteString Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    atomicMapAdd    (opOverMap (+)   :: Op (Map ByteString Int64)) (checkAddition . overlappingMaps)
        , generateTestPropAtomicMapOp "sub"    atomicMapSub    (opOverMap (-)   :: Op (Map ByteString Int64)) (checkSubtraction . overlappingMaps)
        , generateTestPropAtomicMapOp "mul"    atomicMapMul    (opOverMap (*)   :: Op (Map ByteString Int64)) (checkMultiplication . overlappingMaps)
        , generateTestPropAtomicMapOp "div"    atomicMapDiv    (opOverMap div   :: Op (Map ByteString Int64)) (checkIntDivision . overlappingMaps)
        , generateTestPropAtomicMapOp "and"    atomicMapAnd    (opOverMap (.&.) :: Op (Map ByteString Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "or"     atomicMapOr     (opOverMap (.|.) :: Op (Map ByteString Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "xor"    atomicMapXor    (opOverMap xor   :: Op (Map ByteString Int64)) overlappingMaps
        ]
  , testGroup "string-float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" atomicMapInsert (Map.union      :: Op (Map ByteString Double)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" atomicMapDelete (Map.difference :: Op (Map ByteString Double)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    atomicMapAdd    (opOverMap (+)   :: Op (Map ByteString Double)) overlappingMaps
        , generateTestPropAtomicMapOp "sub"    atomicMapSub    (opOverMap (-)   :: Op (Map ByteString Double)) overlappingMaps
        , generateTestPropAtomicMapOp "mul"    atomicMapMul    (opOverMap (*)   :: Op (Map ByteString Double)) overlappingMaps
        , generateTestPropAtomicMapOp "div"    atomicMapDiv    (opOverMap (/)   :: Op (Map ByteString Double)) (checkDoubleDivision . overlappingMaps)
        ]
  , testGroup "string-string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "insert" atomicMapInsert (Map.union      :: Op (Map ByteString ByteString)) nonOverlappingMaps
        , generateTestPropAtomicMapOp "delete" atomicMapDelete (Map.difference :: Op (Map ByteString ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" atomicMapStringPrepend (opOverMap prepend :: Op (Map ByteString ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" atomicMapStringAppend  (opOverMap append  :: Op (Map ByteString ByteString)) overlappingMaps
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

propSearch :: Pool Client -> Text -> NonEmptyBS ByteString -> HyperSerializable -> Property
propSearch clientPool space (NonEmptyBS key) (MkHyperSerializable entry) = QC.monadicIO $ do
  let attributeName = pickAttributeName entry
      attribute = mkAttributeUtf8 (decodeUtf8 attributeName) entry
      keyCheck  = mkAttributeCheckUtf8 (decodeUtf8 keyAttributeName) key HyperpredicateEquals
  QC.run $ join $ withResource clientPool $ \client -> put client space key [attribute]
  searchResults <- QC.run $ withResource clientPool $ \client -> collectSearch client space [keyCheck]
  let resultSet = concat
                $ map (filter ((== attributeName) . attrName))
                $ filter (any (\attr -> attrName attr == keyAttributeName
                                        && attrValue attr == key))
                $ searchResults
  case resultSet == [attribute] of
    True -> QC.assert True
    False -> do
      QC.run $ do
        putStrLn $ "Failed in propSearch"
        putStrLn $ "  attribute:\n" ++ show attribute
        putStrLn $ "  resultSet:\n" ++ show resultSet
        putStrLn $ "  searchResults:\n" ++ show searchResults
      QC.assert False

collectSearch :: Client -> Text -> [AttributeCheck] -> IO [[Attribute]]
collectSearch client space checks = do
  search <- join $ search client space checks
  items <- collect search
  return items
  where
    collect (Left _) = do
       return []
    collect (Right (SearchStream (a, next))) = do
      nextItem <- next
      rest <- collect nextItem
      return $! a : rest
    {-# INLINE collect #-}
{-# INLINE collectSearch #-}

testSearch :: Pool Client -> Text -> Test
testSearch clientPool space = buildTest $ do
  let test = testProperty "search"
             $ propSearch clientPool space
  -- _ <- withResource clientPool $ \client -> deleteGroup client space []
  return test

propDeleteGroup :: Pool Client -> Text -> NonEmptyBS ByteString -> HyperSerializable -> Property
propDeleteGroup clientPool space (NonEmptyBS key) (MkHyperSerializable entry) = QC.monadicIO $ do
  QC.run $ join $ withResource clientPool $ \client -> deleteGroup client space []
  let attributeName = pickAttributeName entry
      attribute = mkAttributeUtf8 (decodeUtf8 attributeName) entry
      attributeCheck = mkAttributeCheckUtf8 (decodeUtf8 attributeName) entry HyperpredicateEquals
  QC.run $ join $ withResource clientPool $ \client -> put client space key [attribute]
  QC.run $ join $ withResource clientPool $ \client -> deleteGroup client space [attributeCheck]
  QC.run $ threadDelay 10000
  searchResults <- QC.run $ withResource clientPool $ \client -> collectSearch client space []
  let resultSet = concat
                $ map (filter ((== attributeName) . attrName))
                $ filter (any (\attr -> attrName attr == keyAttributeName
                                        && attrValue attr == key))
                $ searchResults
  case resultSet == [] of
    True -> QC.assert True
    False -> do
      QC.run $ do
        putStrLn $ "Failed in propDeleteGroup"
        putStrLn $ "  attribute:\n" ++ show attribute
        putStrLn $ "  resultSet:\n" ++ show resultSet
        putStrLn $ "  searchResults:\n" ++ show searchResults
      QC.assert False

testDeleteGroup :: Pool Client -> Text -> Test
testDeleteGroup clientPool space =
  testProperty
    "deleteGroup"
    $ propDeleteGroup clientPool space

propCount :: Pool Client -> Text -> NonEmptyBS ByteString -> HyperSerializable -> Property
propCount clientPool space (NonEmptyBS key) (MkHyperSerializable entry) = QC.monadicIO $ do
  let attributeName = pickAttributeName entry
      attribute = mkAttributeUtf8 (decodeUtf8 attributeName) entry
      keyCheck  = mkAttributeCheckUtf8 (decodeUtf8 keyAttributeName) key HyperpredicateEquals
  preCount <- QC.run $ join $ withResource clientPool $ \client -> count client space [keyCheck]
  QC.run $ join $ withResource clientPool $ \client -> put client space key [attribute]
  postCount <- QC.run $ join $ withResource clientPool $ \client -> count client space [keyCheck]
  case liftA2 (-) postCount preCount of
    Right 1 -> QC.assert True
    _ -> do
      QC.run $ do
        putStrLn $ "Failed in propCount"
        putStrLn $ "  preCount:  " ++ show preCount
        putStrLn $ "  postCount: " ++ show postCount
        putStrLn $ "  attribute:\n" ++ show attribute
      QC.assert False

testCount :: Pool Client -> Text -> Test
testCount clientPool space =
  testProperty
    "count"
    $ propCount clientPool space

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
                [ testSearch
                , testDeleteGroup
                , testCount
                , testAtomic
                ]
  return tests
