-----------------------------------------------------------------------------
-- |
-- Module      :  Data.HyperDex.Client
-- Copyright   :  (c) Aaron Friel 2013
-- License     :  BSD-style
-- Maintainer  :  mayreply@aaronfriel.com
-- Stability   :  maybe
-- Portability :  portable
--
-- A client connection based API for the HyperDex key value store.
--
-- This module exposes functions that rely on manually providing a client
-- parameter, and no guarantees are provided that resources will be cleaned
-- up unless the end-user ensures the connection is closed.
--
-----------------------------------------------------------------------------

module Database.HyperDex.Client
  ( module Database.HyperDex.Internal.Client
  , module Database.HyperDex.Internal.Serialize
  , module Database.HyperDex.Internal.Ffi.Client
  -- Data structures
  , module Database.HyperDex.Internal.Data.Hyperdex
  , module Database.HyperDex.Internal.Data.Attribute
  , module Database.HyperDex.Internal.Data.AttributeCheck
  , module Database.HyperDex.Internal.Data.MapAttribute
  )
  where

import Database.HyperDex.Internal.Ffi.Client
import Database.HyperDex.Internal.Client (ClientConnection, ClientReturnCode, ClientResult, ReturnCode (..), clientConnect)
import Database.HyperDex.Internal.Data.Hyperdex (Hyperdatatype (..), Hyperpredicate (..))
import Database.HyperDex.Internal.Serialize (HyperSerialize, serialize, deserialize, datatype)
import Database.HyperDex.Internal.Data.Attribute (Attribute (..), mkAttribute)
import Database.HyperDex.Internal.Data.AttributeCheck (AttributeCheck (..), mkAttributeCheck)
import Database.HyperDex.Internal.Data.MapAttribute (MapAttribute (..), mkMapAttribute, mkMapAttributesFromMap)
