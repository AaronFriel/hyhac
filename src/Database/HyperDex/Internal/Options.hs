{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module     	: Database.HyperDex.Internal.Options
-- Copyright  	: (c) Aaron Friel 2013-2014
-- License    	: BSD-style
-- Maintainer 	: mayreply@aaronfriel.com
-- Stability  	: unstable
-- Portability	: portable
--
module Database.HyperDex.Internal.Options
	( ConnectInfo (..)
	, ConnectOptions (..)
	, BackoffMethod (..)
	, defaultConnectInfo
	, defaultConnectOptions
	)
	where

import Data.ByteString
import Data.Default
import Data.Word

-- | Parameters for connecting to a HyperDex cluster.
data ConnectInfo =
  ConnectInfo
    { connectHost   	:: {-# UNPACK #-} !ByteString
    , connectPort   	:: {-# UNPACK #-} !Word16
    , connectOptions	:: {-# UNPACK #-} !ConnectOptions
    }
  deriving (Eq, Read, Show)

instance Default ConnectInfo where
  def =
    ConnectInfo
      { connectHost = "127.0.0.1"
      , connectPort = 1982
      , connectOptions = def
      }

defaultConnectInfo :: ConnectInfo
defaultConnectInfo = def

-- | Additional options for connecting and managing the connection
-- to a HyperDex cluster.
data ConnectOptions =
  ConnectOptions
    { connectionBackoff    :: !BackoffMethod
    , connectionBackoffCap :: !(Maybe Int)
    }
  deriving (Eq, Read, Show)

instance Default ConnectOptions where
  def =
    ConnectOptions
      { connectionBackoff		 = BackoffYield -- 10 * 2^n
      , connectionBackoffCap = Just 50000           -- 0.05 seconds.
      }

-- | Sane defaults for HyperDex connection options.
defaultConnectOptions :: ConnectOptions
defaultConnectOptions = def

-- | A connectionBackoff method controls how frequently the client polls internally.
--
-- This is provided to allow fine-tuning performance. Do note that
-- this does not affect any method the hyperdex_client C library uses to poll
-- its connection to a HyperDex cluster.
--
-- All integer values are in microseconds.
data BackoffMethod
  -- | No delay is used except the thread is yielded.
  = BackoffYield
  -- | Delay a constant number of microseconds each inter.
  | BackoffConstant Int
  -- | Delay with an initial number of microseconds, increasing linearly by the second value.
  | BackoffLinear Int Int
  -- | Delay with an initial number of microseconds, increasing exponentially by the second value.
  | BackoffExponential Int Double
  deriving (Eq, Read, Show)

