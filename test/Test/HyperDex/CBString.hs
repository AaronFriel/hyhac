module Test.HyperDex.CBString (cBStringTests)
  where

import Test.Framework (testGroup, Test, plusTestOptions)
import Test.Framework.Options
import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck hiding (NonEmpty)
import Test.QuickCheck.Monadic

import Test.HyperDex.Util

import Data.ByteString (ByteString)

import Database.HyperDex.Internal.Util

import Data.Monoid

canRoundTripArbitraryString :: Test
canRoundTripArbitraryString =
  testProperty "Can round trip an arbitrary string through the CBString API" $
    testRoundTripArbitraryString

testRoundTripArbitraryString :: ByteString -> Property
testRoundTripArbitraryString input = 
  monadicIO $ do
   output <- run $ newCBStringLen input >>= peekCBStringLen 
   return $ output == input

canRoundTripNulTerminatedString :: Test
canRoundTripNulTerminatedString =
  testProperty "Can round trip a non-nul containing string through the CBString API" $
    testRoundTripNulTerminatedString

testRoundTripNulTerminatedString :: NonNul ByteString -> Property
testRoundTripNulTerminatedString (NonNul input) = 
  monadicIO $ do
   output <- run $ newCBString input >>= peekCBString 
   return $ output == input

cBStringTests :: Test
cBStringTests = plusTestOptions (mempty { topt_maximum_generated_tests = Just 1000
                                        , topt_maximum_test_size = Just 65537 } ) $
                makeTestVariety "CBString API Tests"
                  [ canRoundTripArbitraryString
                  , canRoundTripNulTerminatedString
                  ]

smallSizeTests :: Test -> Test
smallSizeTests = plusTestOptions (mempty { topt_maximum_generated_tests = Just 1000
                                         , topt_maximum_test_size = Just 65 } )

mediumSizeTests :: Test -> Test
mediumSizeTests = plusTestOptions (mempty { topt_maximum_generated_tests = Just 1000
                                         , topt_maximum_test_size = Just 1025 } )

-- largeSizeTests :: Test -> Test
-- largeSizeTests = plusTestOptions (mempty { topt_maximum_generated_tests = Just 65
--                                          , topt_maximum_test_size = Just 65537 } )

-- massiveSizeTests :: Test -> Test
-- massiveSizeTests = plusTestOptions (mempty { topt_maximum_generated_tests = Just 5
--                                            , topt_maximum_test_size = Just 1048577 } )

makeTestVariety :: String -> [Test] -> Test
makeTestVariety label tests = 
  testGroup (label <> " of varying size")
  [ smallSizeTests   $ testGroup "Small size tests" tests
  , mediumSizeTests  $ testGroup "Medium size tests" tests
  -- , largeSizeTests   $ testGroup "Large size tests" tests
  -- , massiveSizeTests $ testGroup "Massive size tests" tests
  ]