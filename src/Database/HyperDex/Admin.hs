-- |
-- Module      :  Data.HyperDex.Admin
-- Copyright   :  (c) Aaron Friel 2013
--	              (c) Niklas Hambüchen 2013-2014 
-- License     :  BSD-style
-- Maintainer  :  mayreply@aaronfriel.com
-- Stability   :  maybe
-- Portability :  portable
--
-- TODO description
--
module Database.HyperDex.Admin
  ( module Database.HyperDex.Internal.Admin
  , module Database.HyperDex.Internal.Ffi.Admin
  )
  where

import Database.HyperDex.Internal.Admin (AdminConnection, ReturnCode (..), adminConnect)
import Database.HyperDex.Internal.Ffi.Admin 
