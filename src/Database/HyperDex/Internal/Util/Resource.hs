{-# LANGUAGE FlexibleContexts #-}
-- |
-- Module       : Database.HyperDex.Internal.Util.Resource
-- Copyright    : (c) Aaron Friel 2014
-- License      : BSD-style
-- Maintainer   : mayreply@aaronfriel.com
-- Stability    : unstable
-- Portability  : portable
--

module Database.HyperDex.Internal.Util.Resource
  ( -- Resource Allocation
    rNew
  , rMallocArray
  , rMalloc
  , rNewCBString0
  , rNewCBStringLen
  , unwrapResourceT
  -- Resource Contexts
  , ResourceContext
  , mkContext
  , runInContext
  , closeContext
  )
 where

import Control.Monad
import Control.Monad.Base
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import Control.Monad.Trans.Resource.Internal
import Control.Monad.Trans.Control (control)
import qualified Control.Exception as E
import Data.Acquire

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.ByteString.Unsafe (unsafeUseAsCString, unsafeUseAsCStringLen)

-- See Note [Foreign imports] in Foreign.hs
import Database.HyperDex.Internal.Util.Foreign
import Foreign ( new, free, malloc, mallocArray, mallocArray0, copyBytes
               , newStablePtr, freeStablePtr, pokeElemOff )
import Foreign.C.String 

-- See Note [Resource handling]
hAcquire :: IO a -> (a -> IO ()) -> Acquire a
hAcquire createFn freeFn = mkAcquireType createFn freeFn'
  where 
    freeFn' _ ReleaseException = return ()
    freeFn' x _                = freeFn x
{-# INLINE hAcquire #-}

rNew :: (Storable a, MonadResource m) => a -> m (Ptr a)
rNew a = fmap snd $ allocateAcquire $ hAcquire (new a) free

rMallocArray :: (Storable a, MonadResource m) => Int -> m (Ptr a)
rMallocArray sz = fmap snd $ allocateAcquire $ hAcquire (mallocArray sz) free

rMalloc :: (Storable a, MonadResource m) => m (Ptr a)
rMalloc = fmap snd $ allocateAcquire $ hAcquire malloc free

-- | Run as a ResourceT but do not return resources until explicitly
-- asked, or when freed by the garbage collector.
-- 
-- In the latter case, the release type provided is "ReleaseException".
-- See Note [Resource handling] for details.
--
-- The internal state is 
newtype ResourceContext = ResourceContext InternalState

mkContext :: MonadBase IO m => m ResourceContext
mkContext = liftM ResourceContext createInternalState

runInContext :: MonadBaseControl IO m
             => ResourceContext
             -> ResourceT m a
             -> m a
runInContext (ResourceContext istate) (ResourceT r) = control $ \run ->
  E.mask $ \restore ->
    restore (run (r istate))
             `E.onException` stateCleanup ReleaseException istate

closeContext :: MonadBase IO m => ResourceContext -> m ()
closeContext (ResourceContext istate) = closeInternalState istate 

-- | Use a 'ByteString' as a NUL-terminated 'CString' in 'MonadResource'.
--
-- /O(n) construction/
--
-- This method of construction permits several advantages to 'newCBString'.
--
-- * If the 'ByteString' is already NUL terminated, the internal byte array is
--   returned and no allocation occurs. This permits the consuming client to
--   optimize strings for performance by constructing common strings with NUL
--   terminations.
--
-- * If the string requires NUL termination, a new string is allocated and freed
--   when the resource monad completes or an exception is thrown.
--
-- * When the allocation-less process occurs, to prevent garbage collection, a
--   stable pointer to the parent ByteString is held with a 'StablePtr'. This
--   also utilizes 'MonadResource'.
--
rNewCBString0 :: MonadResource m => ByteString -> m CString
rNewCBString0 bs =
  case isNulTerminated of
    True -> do
      (_, (_, cstr)) <- allocateAcquire $ hAcquire alloc' free'
      return cstr
      where 
        alloc' = unsafeUseAsCString bs $ \cstr -> do
                  ptr <- newStablePtr bs
                  return (ptr, cstr)
        free' (ptr, _) = freeStablePtr ptr
    False -> do
      (_, cstr) <- allocateAcquire $ hAcquire alloc' free'
      return cstr
      where
        alloc' = unsafeUseAsCString bs $ \cstr -> do
                   buf <- mallocArray0 bsLen
                   copyBytes buf cstr bsLen
                   pokeElemOff buf bsLen 0
                   return buf
        free' = free
  where
    bsLen = BS.length bs
    isNulTerminated = bsLen >= 1 && BS.last bs == 0

-- | Use a 'ByteString' as a 'CStringLen' in a 'MonadResource'.
--
-- /O(1) construction/
--
-- This method of construction permits several advantages to 'newCBStringLen'.
--
-- * The internal byte array is used without copying.
--
-- * To prevent garbage collection of the internal storage of the ByteString, a
--   stable pointer to the parent ByteString is held with a 'StablePtr'. This is
--   freed when the resource monad completes or an exception is thrown.
--
rNewCBStringLen :: MonadResource m => ByteString -> m CStringLen
rNewCBStringLen bs = do
  (_, (_, cstrLen)) <- allocateAcquire $ hAcquire alloc' free'
  return cstrLen
  where 
    alloc' = unsafeUseAsCStringLen bs $ \cstrLen -> do
              ptr <- newStablePtr bs
              return (ptr, cstrLen)
    free' (ptr, _) = freeStablePtr ptr

-- | Run a ResourceT in an environment, capturing the resources it allocates and
-- registering them in a parent MonadResource. Return the release key for the 
-- captured resources, and the result of the action.
unwrapResourceT :: (MonadResource m, MonadBase IO m)
                => ResourceT IO a
                -> m (ReleaseKey, a)
unwrapResourceT (ResourceT r) = do
  istate <- createInternalState
  rkey <- register (closeInternalState istate)
  result <- liftIO $ r istate
  return (rkey, result)

{- Note [Resource handling] 
~~~~~~~~~~~~~~~~~~~~~~

Pointers passed to HyperDex must live until the HyperDex library "_loop"
function indicates that the call is complete. Resources in this library
are thus allocated using the "registerType" and "mkAcquireType" functions
which differentiate between normal, early, and failure release conditions.

Consider this pseudocode example:

00    main = do
01      con <- connectToHyperDex
02      asyncResult <- get "Key" con
03      (something within the Hyhac library blows up the connection)
04      result <- asyncResult

If we have freed the pointers that "get" allocates, the HyperDex library
will write to their location regardless, and perform a use-after-free,
which in the worst case could result in code execution.

If for some reason the connection is lost in an error state, the resources
allocated are held indefinitely. A memory leak is the preferred result.

A future version may make this a toggle and permit a fail-fast alternative.
-}