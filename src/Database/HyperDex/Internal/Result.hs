-- |
-- Module       : Database.HyperDex.Internal.Result
-- Copyright    : (c) Aaron Friel 2014
-- License      : BSD-style
-- Maintainer   : mayreply@aaronfriel.com
-- Stability    : unstable
-- Portability  : portable
--
module Database.HyperDex.Internal.Result
  ( Result
	, AsyncResult
	, AsyncResultHandle
	, StreamResultHandle
	, SearchStream (..)
  )
  where

import Database.HyperDex.Internal.Handle

-- | A return value from HyperDex.
type Result r a = IO (Either r a)

-- | A return value used internally by HyperdexClient operations.
--
-- Internally the wrappers to the HyperDex library will return
-- a computation that yields a 'Handle' referring to that request
-- and a continuation that will force the request to return an
-- error in the form of a ReturnCode or a result.
--
-- The result of forcing the result is undefined.
-- The HyperdexClient and its workings are not party to the MVar locking
-- mechanism, and the ReturnCode and/or return value may be in the
-- process of being modified when the computation is forced.
--
-- Consequently, the only safe way to use this is with a wrapper such
-- as 'withClient', which only allows the continuation to be run after
-- the HyperdexClient has returned the corresponding Handle or after the
-- HyperdexClient has been destroyed.
type AsyncResultHandle r a = IO (Handle, Result r a)

-- | A return value used internally by HyperdexClient operations.
--

-- This is the same as 'AsyncResultHandle' except it gives the callback
-- the result of the loop operation that yields the returned 'Handle'.
type StreamResultHandle r a = IO (Handle, Maybe r -> Result r a)

-- | Return values from HyperDex in an asynchronous wrapper.
-- 
-- This is the *only* return type publicly exported.
--
-- The full type is an IO (IO (Either ReturnCode a)). Evaluating
-- the result of an asynchronous call, such as the default get and
-- put operations starts the request to the HyperDex cluster. Evaluating
-- the result of that evaluation will poll internally, using the
-- connection's 'BackoffMethod' until the result is available.
--
-- This API may be deprecated in favor of exclusively using MVars in
-- a future version.
type AsyncResult r a = IO (Result r a)

-- | A return value from HyperDex containing a value and a result that may be
-- evaluated to return additional values.
newtype SearchStream r a = SearchStream (a, Result r (SearchStream r a))
