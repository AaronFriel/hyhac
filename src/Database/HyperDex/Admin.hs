{-# LANGUAGE ViewPatterns #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Data.HyperDex.Admin
-- Copyright   :  (c) Aaron Friel 2013
-- License     :  BSD-style
-- Maintainer  :  mayreply@aaronfriel.com
-- Stability   :  maybe
-- Portability :  portable
--
-- TODO description
--
-----------------------------------------------------------------------------

module Database.HyperDex.Admin
  ( module Database.HyperDex.Internal.Admin
  , module Database.HyperDex.Internal.AdminReturnCode
  , module Database.HyperDex.Internal.Hyperdex
  , module Database.HyperDex.Internal.Space
  )
  where

import Database.HyperDex.Internal.Admin -- TODO restrict exports
import Database.HyperDex.Internal.AdminReturnCode (ReturnCode (..))
import Database.HyperDex.Internal.Hyperdex (Hyperdatatype (..), Hyperpredicate (..))
import Database.HyperDex.Internal.Space -- TODO restrict exports
