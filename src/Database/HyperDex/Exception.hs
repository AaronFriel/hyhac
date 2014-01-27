{-# LANGUAGE DeriveDataTypeable, ExistentialQuantification #-}

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
import Database.HyperDex.Internal.Handle (Handle)

-- | Parent exception in Hyhac exception hierarchy to all others.
--
-- When in doubt, catch this.
data HyperDexException = forall e . Exception e => HyperDexException e
  deriving (Typeable)

instance Show HyperDexException where
  show (HyperDexException e) = show e

instance Exception HyperDexException

fromHyperDexException :: Exception e => e -> SomeException
fromHyperDexException = toException . HyperDexException

toHyperDexException :: Exception e => SomeException -> Maybe e
toHyperDexException x = do
  HyperDexException a <- fromException x
  cast a

-- | Exception thrown if a closed connection is used after being closed.
data ClosedConnectionException = ClosedConnectionException
  deriving (Typeable, Eq)

instance Show ClosedConnectionException where
  show _ = "HyperDex client error - cannot use a closed connection."

instance Exception ClosedConnectionException where
  toException = fromHyperDexException
  fromException = toHyperDexException

-- | Exceptions thrown if the mapping of event handles and callbacks in Hyhac
-- is no longer consistent with the event loop in the HyperDex library.
--
-- Two possibilities:
--
--   * 'HyperDexNonePending' indicates that the HyperDex event loop has no
--     handles active, but the Hyhac handle map is non-empty.
--
--   * 'HyhacNonePending' indicates that the HyperDex event loop returned a
--     handle that does not exist in the Hyhac handle map.
data LoopInconsistentException = HyperDexNonePending [Handle]
                               | HyhacMissingHandle  Handle
  deriving (Typeable, Show, Eq)

instance Exception LoopInconsistentException where
  toException = fromHyperDexException
  fromException = toHyperDexException

-- | Exceptions thrown if the mapping of event handles and callbacks in Hyhac
-- is no longer consistent with the event loop in the HyperDex library.
--
-- Two possibilities:
--
--   * 'HyperDexNonePending' indicates that the HyperDex event loop has no
--     handles active, but the Hyhac handle map is non-empty.
--
--   * 'HyhacNonePending' indicates that the HyperDex event loop returned a
--     handle that does not exist in the Hyhac handle map.
data LoopFatalError a = LoopFatalError a
  deriving (Typeable, Show, Eq)

instance (Typeable a, Show a) => Exception (LoopFatalError a) where
  toException = fromHyperDexException
  fromException = toHyperDexException

-- | Exception thrown if a function that returns a SearchStream unexpectedly
-- retrieves a value after returning a negative handle.
--
-- For this to fire, the implementation of 'returnCodeType' must be
-- non-exhaustive.
--
data SearchResponseUnexpected = SearchResponseUnexpected
  deriving (Typeable, Show, Eq)

instance Exception (SearchResponseUnexpected) where
  toException = fromHyperDexException
  fromException = toHyperDexException
