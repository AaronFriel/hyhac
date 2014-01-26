{-# LANGUAGE DeriveDataTypeable #-}

-- |
-- Module      :  Data.HyperDex.Exception
-- Copyright   :  (c) Aaron Friel 2014
-- License     :  BSD-style
-- Maintainer  :  mayreply@aaronfriel.com
-- Stability   :  maybe
-- Portability :  portable
--
module Database.HyperDex.Exception
  where

import Control.Exception
import Data.Typeable

data HyperDexException = ClosedConnectionException
                       | LoopInconsistency
                       | LoopFatalError
                       | ReturnCodeNonExhaustive
	deriving (Show, Typeable)

instance Exception HyperDexException
