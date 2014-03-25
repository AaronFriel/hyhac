{-# LANGUAGE DeriveDataTypeable, GeneralizedNewtypeDeriving #-}
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
  , mkHandle
  , unHandle
  , handleSuccess
  , invalidHandle
  , wrapHyperCallHandle
  , HandleMap
  , empty, null
  , insert, delete
  , lookup, member
  , keys, elems
  , toList
  )
  where

import Prelude hiding (null, lookup)

import Database.HyperDex.Internal.Util (wrapHyperCall)

import Foreign
import Foreign.C
import Data.Typeable
import Data.Hashable
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
newtype Handle = Handle Int64
  deriving (Show, Eq, Ord, Typeable, Hashable)

mkHandle :: CLong -> Handle
mkHandle (CLong x) = Handle x

unHandle :: Handle -> CLong
unHandle (Handle x) = CLong x

handleSuccess :: Handle -> Bool
handleSuccess (Handle x) = x >= 0

invalidHandle :: Handle
invalidHandle = Handle minBound

wrapHyperCallHandle :: IO CLong -> IO Handle
wrapHyperCallHandle = fmap mkHandle . wrapHyperCall
{-# INLINE wrapHyperCallHandle #-}

-- | A mapping of handles to callbacks functions that perform cleanup and return
-- the asynchronous values to their caller.
type HandleMap a = HashMap Handle a

empty :: HandleMap a
empty = HashMap.empty

null :: HandleMap a -> Bool
null = HashMap.null

insert :: Handle -> a -> HandleMap a -> HandleMap a
insert k v = HashMap.insert k v

delete :: Handle -> HandleMap a -> HandleMap a
delete k = HashMap.delete k

lookup :: Handle -> HandleMap a -> Maybe a
lookup k = HashMap.lookup k

member :: Handle -> HandleMap a -> Bool
member k = HashMap.member k

elems :: HandleMap a -> [a]
elems = HashMap.elems

keys :: HandleMap a -> [Handle]
keys = HashMap.keys

toList :: HandleMap a -> [(Handle, a)]
toList = HashMap.toList
