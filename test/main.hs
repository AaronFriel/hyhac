module Main ( main ) where

import Test.Framework (defaultMain, testGroup)
import Test.Framework.Providers.HUnit
import Test.Framework.Providers.QuickCheck2
import Test.HUnit

import Database.HyperDex
import Test.HyperDex.Internal (internalTests)
import Test.HyperDex.CBString (cBStringTests)

testVersion :: Assertion
testVersion =
	assertEqual "hyhac-version" "0.1.0.0" hyhacVersion

main = defaultMain
				[ testCase "Version match" testVersion
        , internalTests
        , cBStringTests
				]
