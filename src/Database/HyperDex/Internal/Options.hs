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
	, defaultConnectInfo
	)
	where

import Data.ByteString
import Data.Default
import Data.Word

-- | Parameters for connecting to a HyperDex cluster.
data ConnectInfo =
  ConnectInfo
    { connectHost	:: {-# UNPACK #-} !ByteString
    , connectPort	:: {-# UNPACK #-} !Word16
    }
  deriving (Eq, Read, Show)

instance Default ConnectInfo where
  def =
    ConnectInfo
      { connectHost = "127.0.0.1"
      , connectPort = 1982
      }

defaultConnectInfo :: ConnectInfo
defaultConnectInfo = def
