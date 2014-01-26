{-# LANGUAGE NoImplicitPrelude #-}

-- |
-- Module       : Database.HyperDex.Internal.Handle
-- Copyright    : (c) Aaron Friel 2014
-- License      : BSD-style
-- Maintainer   : mayreply@aaronfriel.com
-- Stability    : unstable
-- Portability  : portable
--
module Database.HyperDex.Internal.Handle
  ( Handle
	, HandleMap
	, HandleCallback (..)
	, empty, null
	, insert, delete
	, lookup, member
	, elems
  )
  where

import Prelude hiding (null, lookup)

import Foreign
import Foreign.C
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap

-- | Primary return value from hyperdex operations.
--
-- This is the proper "return value" of hyperdex library functions, which for
-- successful calls is an index that will refer to a pending asynchronous
-- operation. 
--
-- Per the specification, it's guaranteed to be a unique integer for each
-- outstanding operation using a given client. In practice it is monotonically 
-- increasing while operations are outstanding, lower values are used first, and
-- negative values represent an error.
type Handle = CLong

-- | A mapping of handles to callbacks functions that perform cleanup and return
-- the asynchronous values to their caller.
newtype HandleMap = HandleMap { unHandleMap :: HashMap Int64 HandleCallback }

-- | A callback used to perform work when the HyperdexClient loop indicates an
-- operation has been completed.
--
-- The callback is passed either:
--
--   * 'Nothing', indicating that execution has halted
--
--   * 'Just r', indicating the return code of the operation
--
-- In each case the callback may return either:
--
--   * 'Nothing', indicating that no further execution is necessary
--
--   * 'Just (h, callback)', indicating a new execution handle and callback
--     should be registered.
--
newtype HandleCallback = 
  HandleCallback (Maybe Int -> IO (Maybe (Handle, HandleCallback)))

unCLong :: CLong -> Int64
unCLong (CLong a) = a

empty :: HandleMap
empty = HandleMap $ HashMap.empty

null :: HandleMap -> Bool
null = HashMap.null . unHandleMap

insert :: Handle -> HandleCallback -> HandleMap -> HandleMap
insert k v = HandleMap . HashMap.insert (unCLong k) v . unHandleMap

delete :: Handle -> HandleMap -> HandleMap
delete k = HandleMap . HashMap.delete (unCLong k) . unHandleMap

lookup :: Handle -> HandleMap -> Maybe HandleCallback
lookup k = HashMap.lookup (unCLong k) . unHandleMap

member :: Handle -> HandleMap -> Bool
member k = HashMap.member (unCLong k) . unHandleMap

elems :: HandleMap -> [HandleCallback]
elems = HashMap.elems . unHandleMap
