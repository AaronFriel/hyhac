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
-- import Database.HyperDex.Internal.Util

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
import Control.DeepSeq
import Control.Exception.Base

testCanStoreLargeObject :: Pool (ClientConnection) -> Test
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
  result <- join $ withResource clientPool $ put defaultSpace "large" attrs
  assertEqual "Remove space: " (Right ()) result

getResult :: Text -> Either ClientReturnCode [Attribute] -> Either String Attribute
getResult _         (Left returnCode) = Left $ "Failure, returnCode: " <> show returnCode
getResult attribute (Right attrList)  =
  case (filter (\a -> attrName a == encodeUtf8 attribute) attrList) of
          [x] -> Right x
          []  -> Left $ "No valid attribute, attributes list: " <> show attrList
          _   -> Left "More than one returned value"

putHyper :: Pool ClientConnection -> ByteString -> ByteString -> Attribute -> QC.PropertyM IO (Either ClientReturnCode ())
putHyper clientPool space key attribute = do
    QC.run . join $ withResource clientPool $ put space key [attribute]

getHyper :: Pool ClientConnection -> ByteString -> ByteString -> Text -> QC.PropertyM IO (Either String Attribute)
getHyper clientPool space key attribute = do
    eitherAttrList <- QC.run . join $ withResource clientPool $ get space key
    let retValue = getResult attribute eitherAttrList
    case retValue of
      Left err -> QC.run $ do
        putStrLn $ "getHyper encountered error: " <> show err
        putStrLn $ "Attribute: "
        putStrLn $ show . fmap (filter (\x -> decodeUtf8 (attrName x) == attribute)) $ eitherAttrList
      _ -> return ()
    return $ retValue

propCanStore :: HyperSerialize a => Pool (ClientConnection) -> ByteString -> a
                -> ByteString -> NonEmptyBS ByteString -> Property
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

propCanConditionalPutNumeric :: Pool ClientConnection -> ByteString
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
      _ <- QC.run . join $ withResource clientPool $ delete space key
      _ <- QC.run . join $ withResource clientPool $
             put space key [initialAttribute]
      failingResult <- QC.run . join $ withResource clientPool $
                          putConditional space key [failingAttributeCheck] [failingAttribute]
      case failingResult of
        Left ClientCmpfail -> return ()
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
             putConditional space key [succeedingAttributeCheck] [succeedingAttribute]
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

type AsyncOp = ByteString -> ByteString -> [Attribute] -> ClientConnection -> IO (ClientResult ())

generateTestPropAtomicOp :: (Show a, Eq a, HyperSerialize a,
                             Show b, Eq b, HyperSerialize b,
                             Show x, Arbitrary x)
                         => String         -- ^ The test name
                         -> AsyncOp        -- ^ The HyperDex operation to be performed
                         -> (a -> b -> a)  -- ^ The operation used to simulate execution
                         -> (x -> (a, b))  -- ^ The deconstructor for the arbitrary type
                         -> (Pool ClientConnection -> ByteString -> Test)
generateTestPropAtomicOp testName hyperCall localOp decons =
  \clientPool space -> testProperty testName $
    \(NonEmptyBS key) arbitraryInput ->
      QC.monadicIO $ do
      let (initial, operand) = decons arbitraryInput
          attributeName      = decodeUtf8 $ pickAttributeName initial
          attribute          = mkAttributeUtf8 attributeName initial
          opAttribute        = mkAttributeUtf8 attributeName operand
      _ <- QC.run . join $ withResource clientPool $ delete space key
      _ <- QC.run . join $ withResource clientPool $ put space key [attribute]
      atomicOpResult <- QC.run . join $ withResource clientPool $ hyperCall space key [opAttribute]
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

type AsyncMapOp = ByteString -> ByteString -> [MapAttribute] -> ClientConnection -> IO (ClientResult ())

generateTestPropAtomicMapOp :: (Show k, Show v, Eq k, Eq v, NFData v, NFData k, HyperSerialize k, HyperSerialize v,
                                HyperSerialize (Map k v),
                                Show x, Arbitrary x)
                            => String         -- ^ The test name
                            -> AsyncMapOp     -- ^ The HyperDex operation to be performed
                            -> (Map k v -> Map k v -> Map k v)  -- ^ The operation used to simulate execution
                            -> (x -> (Map k v, Map k v))  -- ^ The deconstructor for the arbitrary type
                            -> (Pool ClientConnection -> ByteString -> Test)
generateTestPropAtomicMapOp testName hyperCall localOp' decons =
  \clientPool space -> testProperty testName $
    \(NonEmptyBS key) arbitraryInput ->
      QC.monadicIO $ do
      let (initial', operand') = decons arbitraryInput
          initial = force initial'
          operand = force operand'
          localOp = localOp' `seq` localOp'
          attributeName      = force $ decodeUtf8 $ pickAttributeName initial
          attribute          = force $ mkAttributeUtf8 attributeName initial
          opAttribute        = force $ mkMapAttributesFromMapUtf8 attributeName operand
      _ <- QC.run . join $ withResource clientPool $ delete space key
      _ <- QC.run . join $ withResource clientPool $ put space key [attribute]
      atomicOpResult <- QC.run . join $ withResource clientPool $ hyperCall space key opAttribute
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

testAtomic :: Pool ClientConnection -> ByteString -> Test
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

testAtomicInteger :: Pool ClientConnection -> ByteString -> Test
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

testAtomicFloat :: Pool ClientConnection -> ByteString -> Test
testAtomicFloat clientPool space =
  testGroup "float"
  $ fmap (\f -> f clientPool space)
    [ generateTestPropAtomicOp "add" atomicAdd ((+) :: Op Double) id
    , generateTestPropAtomicOp "sub" atomicSub ((-) :: Op Double) id
    , generateTestPropAtomicOp "mul" atomicMul ((*) :: Op Double) id
    , generateTestPropAtomicOp "div" atomicDiv ((/) :: Op Double) safeDivideDouble
    ]

testAtomicString :: Pool ClientConnection -> ByteString -> Test
testAtomicString clientPool space =
  testGroup "string"
  $ fmap (\f -> f clientPool space)
    [ generateTestPropAtomicOp "prepend" stringPrepend prepend id
    , generateTestPropAtomicOp "append"  stringAppend  append  id
    ]
  where prepend = flip append

testAtomicList :: Pool ClientConnection -> ByteString -> Test
testAtomicList clientPool space =
  testGroup "list"
  [ testGroup "int"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "lpush" listLPush (prepend :: Op2 [Int64] Int64) id
        , generateTestPropAtomicOp "rpush" listRPush (append  :: Op2 [Int64] Int64) id
        ]
  , testGroup "float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "lpush" listLPush (prepend :: Op2 [Double] Double) id
        , generateTestPropAtomicOp "rpush" listRPush (append  :: Op2 [Double] Double) id
        ]
  , testGroup "string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "lpush" listLPush (prepend :: Op2 [ByteString] ByteString) id
        , generateTestPropAtomicOp "rpush" listRPush (append  :: Op2 [ByteString] ByteString) id
        ]
  ]
  where append, prepend :: Op2 [a] a
        append  a b = a ++ [b]
        prepend a b = [b] ++ a

testAtomicSet :: Pool ClientConnection -> ByteString -> Test
testAtomicSet clientPool space =
  testGroup "set"
  [ testGroup "int"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "add"       setAdd        (flip Set.insert  :: Op2 (Set Int64) Int64) id
        , generateTestPropAtomicOp "remove"    setRemove     (flip Set.delete  :: Op2 (Set Int64) Int64) id
        , generateTestPropAtomicOp "intersect" setIntersect  (Set.intersection :: Op (Set Int64)) id
        , generateTestPropAtomicOp "union"     setUnion      (Set.union        :: Op (Set Int64)) id
        ]
  , testGroup "float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "add"       setAdd        (flip Set.insert  :: Op2 (Set Double) Double) id
        , generateTestPropAtomicOp "remove"    setRemove     (flip Set.delete  :: Op2 (Set Double) Double) id
        , generateTestPropAtomicOp "intersect" setIntersect  (Set.intersection :: Op (Set Double)) id
        , generateTestPropAtomicOp "union"     setUnion      (Set.union        :: Op (Set Double)) id
        ]
  , testGroup "string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicOp "add"       setAdd        (flip Set.insert  :: Op2 (Set ByteString) ByteString) id
        , generateTestPropAtomicOp "remove"    setRemove     (flip Set.delete  :: Op2 (Set ByteString) ByteString) id
        , generateTestPropAtomicOp "intersect" setIntersect  (Set.intersection :: Op (Set ByteString)) id
        , generateTestPropAtomicOp "union"     setUnion      (Set.union        :: Op (Set ByteString)) id
        ]
  ]

opOverMap :: Ord k => (a -> b -> a) -> Map k a -> Map k b -> Map k a
opOverMap operator initials operands = Map.mapWithKey go initials
  where
    go k a = case k `Map.lookup` operands of
                Just b  -> a `operator` b
                Nothing -> a

testAtomicMap :: Pool ClientConnection -> ByteString -> Test
testAtomicMap clientPool space =
  testGroup "map"
  [ testGroup "int-int"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "union"  mapAdd          (flip Map.union  :: Op (Map Int64 Int64)) id
        -- TODO: Implement mapAtomicInsert, mapAtomicDelete (conditional insert?)
        -- , generateTestPropAtomicOp    "remove" mapRemove       (flip Map.delete :: Op2 (Map Int64 Int64) Int64) id
        , generateTestPropAtomicMapOp "add"    mapAtomicAdd    (opOverMap (+)   :: Op (Map Int64 Int64)) (checkAddition . overlappingMaps)
        , generateTestPropAtomicMapOp "sub"    mapAtomicSub    (opOverMap (-)   :: Op (Map Int64 Int64)) (checkSubtraction . overlappingMaps)
        , generateTestPropAtomicMapOp "mul"    mapAtomicMul    (opOverMap (*)   :: Op (Map Int64 Int64)) (checkMultiplication . overlappingMaps)
        , generateTestPropAtomicMapOp "div"    mapAtomicDiv    (opOverMap div   :: Op (Map Int64 Int64)) (checkIntDivision . overlappingMaps)
        , generateTestPropAtomicMapOp "and"    mapAtomicAnd    (opOverMap (.&.) :: Op (Map Int64 Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "or"     mapAtomicOr     (opOverMap (.|.) :: Op (Map Int64 Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "xor"    mapAtomicXor    (opOverMap xor   :: Op (Map Int64 Int64)) overlappingMaps
        ]
  , testGroup "int-float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "union"  mapAdd          (flip Map.union  :: Op (Map Int64 Double)) id
        -- , generateTestPropAtomicOp    "delete" mapAtomicDelete (Map.difference :: Op (Map Int64 Double)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    mapAtomicAdd    (opOverMap (+)   :: Op (Map Int64 Double)) overlappingMaps
        , generateTestPropAtomicMapOp "sub"    mapAtomicSub    (opOverMap (-)   :: Op (Map Int64 Double)) overlappingMaps
        , generateTestPropAtomicMapOp "mul"    mapAtomicMul    (opOverMap (*)   :: Op (Map Int64 Double)) overlappingMaps
        , generateTestPropAtomicMapOp "div"    mapAtomicDiv    (opOverMap (/)   :: Op (Map Int64 Double)) (checkDoubleDivision . overlappingMaps)
        ]
  , testGroup "int-string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "union"  mapAdd          (flip Map.union  :: Op (Map Int64 ByteString)) id
        -- , generateTestPropAtomicOp    "delete" mapAtomicDelete (Map.difference :: Op (Map Int64 ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" mapStringPrepend (opOverMap prepend :: Op (Map Int64 ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" mapStringAppend  (opOverMap append  :: Op (Map Int64 ByteString)) overlappingMaps
        ]
  ,  testGroup "float-int"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "union"  mapAdd          (flip Map.union  :: Op (Map Double Int64)) id
        -- , generateTestPropAtomicOp    "delete" mapAtomicDelete (Map.difference  :: Op (Map Double Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    mapAtomicAdd    (opOverMap (+)   :: Op (Map Double Int64)) (checkAddition . overlappingMaps)
        , generateTestPropAtomicMapOp "sub"    mapAtomicSub    (opOverMap (-)   :: Op (Map Double Int64)) (checkSubtraction . overlappingMaps)
        , generateTestPropAtomicMapOp "mul"    mapAtomicMul    (opOverMap (*)   :: Op (Map Double Int64)) (checkMultiplication . overlappingMaps)
        , generateTestPropAtomicMapOp "div"    mapAtomicDiv    (opOverMap div   :: Op (Map Double Int64)) (checkIntDivision . overlappingMaps)
        , generateTestPropAtomicMapOp "and"    mapAtomicAnd    (opOverMap (.&.) :: Op (Map Double Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "or"     mapAtomicOr     (opOverMap (.|.) :: Op (Map Double Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "xor"    mapAtomicXor    (opOverMap xor   :: Op (Map Double Int64)) overlappingMaps
        ]
  , testGroup "float-float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "union"  mapAdd          (flip Map.union  :: Op (Map Double Double)) id
        -- , generateTestPropAtomicOp    "delete" mapAtomicDelete (Map.difference :: Op (Map Double Double)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    mapAtomicAdd    (opOverMap (+)   :: Op (Map Double Double)) overlappingMaps
        , generateTestPropAtomicMapOp "sub"    mapAtomicSub    (opOverMap (-)   :: Op (Map Double Double)) overlappingMaps
        , generateTestPropAtomicMapOp "mul"    mapAtomicMul    (opOverMap (*)   :: Op (Map Double Double)) overlappingMaps
        , generateTestPropAtomicMapOp "div"    mapAtomicDiv    (opOverMap (/)   :: Op (Map Double Double)) (checkDoubleDivision . overlappingMaps)
        ]
  , testGroup "float-string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "union"  mapAdd          (flip Map.union  :: Op (Map Double ByteString)) id
        -- , generateTestPropAtomicOp    "delete" mapAtomicDelete (Map.difference :: Op (Map Double ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" mapStringPrepend (opOverMap prepend :: Op (Map Double ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" mapStringAppend  (opOverMap append  :: Op (Map Double ByteString)) overlappingMaps
        ]
  ,  testGroup "string-int"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "union"  mapAdd          (flip Map.union  :: Op (Map ByteString Int64)) id
        -- , generateTestPropAtomicOp    "delete" mapAtomicDelete (Map.difference  :: Op (Map ByteString Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    mapAtomicAdd    (opOverMap (+)   :: Op (Map ByteString Int64)) (checkAddition . overlappingMaps)
        , generateTestPropAtomicMapOp "sub"    mapAtomicSub    (opOverMap (-)   :: Op (Map ByteString Int64)) (checkSubtraction . overlappingMaps)
        , generateTestPropAtomicMapOp "mul"    mapAtomicMul    (opOverMap (*)   :: Op (Map ByteString Int64)) (checkMultiplication . overlappingMaps)
        , generateTestPropAtomicMapOp "div"    mapAtomicDiv    (opOverMap div   :: Op (Map ByteString Int64)) (checkIntDivision . overlappingMaps)
        , generateTestPropAtomicMapOp "and"    mapAtomicAnd    (opOverMap (.&.) :: Op (Map ByteString Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "or"     mapAtomicOr     (opOverMap (.|.) :: Op (Map ByteString Int64)) overlappingMaps
        , generateTestPropAtomicMapOp "xor"    mapAtomicXor    (opOverMap xor   :: Op (Map ByteString Int64)) overlappingMaps
        ]
  , testGroup "string-float"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "union"  mapAdd          (flip Map.union  :: Op (Map ByteString Double)) id
        --, generateTestPropAtomicOp    "delete" mapAtomicDelete (Map.difference  :: Op (Map ByteString Double)) overlappingMaps
        , generateTestPropAtomicMapOp "add"    mapAtomicAdd    (opOverMap (+)   :: Op (Map ByteString Double)) overlappingMaps
        , generateTestPropAtomicMapOp "sub"    mapAtomicSub    (opOverMap (-)   :: Op (Map ByteString Double)) overlappingMaps
        , generateTestPropAtomicMapOp "mul"    mapAtomicMul    (opOverMap (*)   :: Op (Map ByteString Double)) overlappingMaps
        , generateTestPropAtomicMapOp "div"    mapAtomicDiv    (opOverMap (/)   :: Op (Map ByteString Double)) (checkDoubleDivision . overlappingMaps)
        ]
  , testGroup "string-string"
    $ fmap (\f -> f clientPool space)
        [ generateTestPropAtomicMapOp "union"  mapAdd          (flip Map.union  :: Op (Map ByteString ByteString)) id
        --, generateTestPropAtomicOp    "delete" mapAtomicDelete (Map.difference :: Op (Map ByteString ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" mapStringPrepend (opOverMap prepend :: Op (Map ByteString ByteString)) overlappingMaps
        , generateTestPropAtomicMapOp "prepend" mapStringAppend  (opOverMap append  :: Op (Map ByteString ByteString)) overlappingMaps
        ]
  ]
  where prepend = flip append

createAction = do
  clientConnect defaultConnectInfo

closeAction _ = do
  -- TODO: Implement disconnect/close?
  -- close client
  return ()

mkPool = createPool createAction closeAction 1 0.5 1

testCanRoundtrip :: Pool (ClientConnection) -> Test
testCanRoundtrip clientPool =
  testProperty
    "roundtrip"
    $ \(MkHyperSerializable value) -> propCanStore clientPool "arbitrary" value defaultSpace

testConditional :: Pool (ClientConnection) -> Test
testConditional clientPool =
  testProperty
    "conditional"
    $ propCanConditionalPutNumeric clientPool defaultSpace

propSearch :: Pool ClientConnection -> ByteString -> NonEmptyBS ByteString -> HyperSerializable -> Property
propSearch clientPool space (NonEmptyBS key) (MkHyperSerializable entry) = QC.monadicIO $ do
  let attributeName = pickAttributeName entry
      attribute = mkAttributeUtf8 (decodeUtf8 attributeName) entry
      keyCheck  = mkAttributeCheckUtf8 (decodeUtf8 keyAttributeName) key HyperpredicateEquals
  QC.run $ join $ withResource clientPool $ put space key [attribute]
  searchResults <- QC.run $ withResource clientPool $ collectSearch space [keyCheck]
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
        putStrLn $ "  key:\n" ++ show key
        putStrLn $ "  attributeCheck:\n" ++ show keyCheck
        putStrLn $ "  resultSet:\n" ++ show resultSet
        putStrLn $ "  searchResults:\n" ++ show searchResults
      QC.assert False

collectSearch :: ByteString -> [AttributeCheck] -> ClientConnection -> IO [[Attribute]]
collectSearch space checks client = do
  stream <- search space checks client
  items <- collect stream
  return items
  where
    -- TODO: Implement helper to work with streams.
    -- This method is not efficient.
    collect stream = do
      result <- readStream stream
      case result of
        Nothing  -> return []
        Just (Left _) -> return []
        Just (Right value) -> do
          rest <- collect stream
          return $! value : rest
    {-# INLINE collect #-}
{-# INLINE collectSearch #-}

testSearch :: Pool ClientConnection -> ByteString -> Test
testSearch clientPool space = buildTest $ do
  -- _ <- withResource clientPool $ deleteGroup space []
  let test = testProperty "search"
             $ propSearch clientPool space
  return test

propDeleteGroup :: Pool ClientConnection -> ByteString -> NonEmptyBS ByteString -> HyperSerializable -> Property
propDeleteGroup clientPool space (NonEmptyBS key) (MkHyperSerializable entry) = QC.monadicIO $ do
  --QC.run $ join $ withResource clientPool $ deleteGroup space []
  let attributeName = pickAttributeName entry
      attribute = mkAttributeUtf8 (decodeUtf8 attributeName) entry
      attributeCheck = mkAttributeCheckUtf8 (decodeUtf8 attributeName) entry HyperpredicateEquals
  QC.run $ join $ withResource clientPool $ put space key [attribute]
  QC.run $ join $ withResource clientPool $ deleteGroup space [attributeCheck]
  QC.run $ threadDelay 10000
  searchResults <- QC.run $ withResource clientPool $ collectSearch space []
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

testDeleteGroup :: Pool (ClientConnection) -> ByteString -> Test
testDeleteGroup clientPool space =
  testProperty
    "deleteGroup"
    $ propDeleteGroup clientPool space

propCount :: Pool (ClientConnection) -> ByteString -> NonEmptyBS ByteString -> HyperSerializable -> Property
propCount clientPool space (NonEmptyBS key) (MkHyperSerializable entry) = QC.monadicIO $ do
  let attributeName = pickAttributeName entry
      attribute = mkAttributeUtf8 (decodeUtf8 attributeName) entry
      keyCheck  = mkAttributeCheckUtf8 (decodeUtf8 keyAttributeName) key HyperpredicateEquals
  preCount <- QC.run $ join $ withResource clientPool $ count space [keyCheck]
  QC.run $ join $ withResource clientPool $ put space key [attribute] 
  postCount <- QC.run $ join $ withResource clientPool $ count space [keyCheck]
  case (preCount, liftA2 (-) postCount preCount) of
    (Right 0, Right 1) -> QC.assert True
    (Right 1, Right 0) -> QC.assert True
    _ -> do
      QC.run $ do
        putStrLn $ "Failed in propCount"
        putStrLn $ "  preCount:  " ++ show preCount
        putStrLn $ "  postCount: " ++ show postCount
        putStrLn $ "  attribute:\n" ++ show attribute
      QC.assert False

testCount :: Pool (ClientConnection) -> ByteString -> Test
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
                [ 
                  testSearch
                -- , testDeleteGroup
                , testCount
                , testAtomic
                ]
  return tests
