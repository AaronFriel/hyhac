{-# LANGUAGE DeriveDataTypeable, NoImplicitPrelude #-}

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
  , handleToCLong
  , handleFromCLong
  , handleSuccess
  , HandleMap
  , HandleCallback (..)
  , empty, null
  , insert, delete
  , lookup, member
  , keys, elems
  )
  where

import Prelude hiding (null, lookup)

import Foreign
import Foreign.C
import Data.Typeable
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
newtype Handle = Handle { unHandle :: Int64 }
  deriving (Show, Eq, Ord, Typeable)

handleFromCLong :: CLong -> Handle
handleFromCLong (CLong x) = Handle x

handleToCLong :: Handle -> CLong
handleToCLong (Handle x) = CLong x

handleSuccess :: Handle -> Bool
handleSuccess (Handle x) = x >= 0

-- | A mapping of handles to callbacks functions that perform cleanup and return
-- the asynchronous values to their caller.
newtype HandleMap = HandleMap { unHandleMap :: HashMap Int64 HandleCallback }

-- | A callback used to perform work when the HyperdexClient loop indicates an
-- operation has been completed.
--
-- The first parameter is a cleanup function to free any allocations.
--
-- The second parameter is a continuation defined by the wrapper around the C
-- library function.
--
data HandleCallback = HandleCallback
  { callbackCleanup      :: !(IO ())
  , callbackContinuation :: !(IO ())
  }

empty :: HandleMap
empty = HandleMap $ HashMap.empty

null :: HandleMap -> Bool
null = HashMap.null . unHandleMap

insert :: Handle -> HandleCallback -> HandleMap -> HandleMap
insert k v = HandleMap . HashMap.insert (unHandle k) v . unHandleMap

delete :: Handle -> HandleMap -> HandleMap
delete k = HandleMap . HashMap.delete (unHandle k) . unHandleMap

lookup :: Handle -> HandleMap -> Maybe HandleCallback
lookup k = HashMap.lookup (unHandle k) . unHandleMap

member :: Handle -> HandleMap -> Bool
member k = HashMap.member (unHandle k) . unHandleMap

elems :: HandleMap -> [HandleCallback]
elems = HashMap.elems . unHandleMap

keys :: HandleMap -> [Handle]
keys = map Handle . HashMap.keys . unHandleMap
