module Main ( main ) where

import Test.Framework (defaultMain, testGroup)
import Test.Framework.Runners.Console
import Test.Framework.Runners.Options
import Test.Framework.Providers.HUnit
import Test.Framework.Providers.QuickCheck2
import Test.HUnit

import Database.HyperDex
import Test.HyperDex.Internal (internalTests)
import Test.HyperDex.CBString (cBStringTests)

import Data.Monoid

testVersion :: Assertion
testVersion =
	assertEqual "hyhac-version" "0.2.0.0" hyhacVersion

main = defaultMainWithOpts
				[ testCase "Version match" testVersion
        , internalTests
        , cBStringTests
				]
        (mempty { ropt_hide_successes = Just False}) 
