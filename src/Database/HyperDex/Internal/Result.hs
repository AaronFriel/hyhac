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
	, SearchStream (..)
  )
  where

import Database.HyperDex.Internal.Handle

-- | A return value from HyperDex.
type Result a = IO (Either Int a)

-- | A return value used internally by HyperdexClient operations.
--
-- Internally the wrappers to the HyperDex library will return
-- a computation that yields a 'Handle' referring to that request
-- and a continuation that will force the request to return an
-- error in the form of a ReturnCode or a result.
--
--
type AsyncResultHandle a =
	IO (Handle, IO (), Result a)

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
type AsyncResult a = IO (Result a)

-- | A return value from HyperDex containing a value and a result that may be
-- evaluated to return additional values.
newtype SearchStream a = SearchStream (Result (a, SearchStream a))
