{-# LANGUAGE CPP, ScopedTypeVariables, FlexibleContexts #-}

-- |
-- Module       : Database.HyperDex.Internal.Util
-- Copyright    : (c) Aaron Friel 2013-2014
--                (c) Niklas Hambüchen 2013-2014
--                (c) Mark Wotton 2013-2014
-- License      : BSD-style
-- Maintainer   : mayreply@aaronfriel.com
-- Stability    : unstable
-- Portability  : portable
--

module Database.HyperDex.Internal.Util
 ( wrapHyperCall
 , newCBString, newCBStringLen
 , withTextUtf8
 , newTextUtf8
 , peekTextUtf8
 -- Queues
 , InTQueue
 , OutTQueue
 , newSplitTQueueIO
 , writeOutTQueue
 , writeOutTQueueIO
 , readInTQueue
 , readInTQueueIO
 -- MonadResource methods:
 , rNew
 , rMalloc
 , rMallocArray
 , rNewCBString0
 , rNewCBStringLen
 , unwrapResourceT
 , tryUnwrapResourceT
 -- Miscellany
 , forkIO_
 )
 where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.Base
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import Control.Monad.Trans.Resource.Internal
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.ByteString.Unsafe (unsafeUseAsCString, unsafeUseAsCStringLen)
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import Foreign hiding (void)
import Foreign.C

#ifdef __UNIX__
import System.Posix.Signals (reservedSignals, blockSignals, unblockSignals)
#endif

wrapHyperCall :: IO a -> IO a
#ifdef __UNIX__
wrapHyperCall f = do
  blockSignals reservedSignals
  r <- f
  unblockSignals reservedSignals
  return r
#else
wrapHyperCall = id
#endif
{-# INLINE wrapHyperCall #-}

-- | Marshal a ByteString into a NUL terminated C string.
--
-- * the ByteString string /must not/ contain any NUL characters
--
-- * as with 'newCAString', new storage is allocated for the C String
--   and must be explicitly freed
--
newCBString :: ByteString -> IO CString
newCBString bs = unsafeUseAsCString bs
                 (\cs -> do
                     let l = BS.length bs
                     buf <- mallocArray0 l
                     copyBytes buf cs l
                     pokeElemOff buf l 0
                     return buf)
{-# INLINE newCBString #-}

-- | Marshal a ByteString into a C string with explicit length.
--
-- * as with 'newCStringLen', new storage is allocated for the C String
--   and must be explicitly freed
--
newCBStringLen :: ByteString -> IO CStringLen
newCBStringLen bs = unsafeUseAsCStringLen bs
                    (\(cs,l) -> do
                      buf <- mallocArray l
                      copyBytes buf cs l
                      return (buf,l))
{-# INLINE newCBStringLen #-}

-- | Marshal a Text field as a UTF8 C string in temporary storage.
--
-- * the memory is freed when the subcomputation terminates (either
--   normally or via an exception), so the pointer to the temporary
--   storage /must not/ be used after this.
--
withTextUtf8 :: Text -> (CString -> IO a) -> IO a
withTextUtf8 = BS.useAsCString . encodeUtf8
{-# INLINE withTextUtf8 #-}

-- | Marshal a Text field as a UTF8 C string.
--
-- * the Text input /must not/ contain any NUL characters
--
-- * as with 'newCAString', new storage is allocated for the C String
--   and must be explicitly freed
--
-- TODO: version that avoids double encode.
newTextUtf8 :: Text -> IO CString
newTextUtf8 = newCBString . encodeUtf8
{-# INLINE newTextUtf8 #-}

peekTextUtf8 :: Ptr CString -> IO Text
peekTextUtf8 ptr = do
  bstring <- peek ptr >>= BS.packCString
  return $ decodeUtf8 bstring 
{-# INLINE peekTextUtf8 #-} 


-- | Unidirectional queue (input only)
newtype InTQueue a = InTQueue (TQueue a)

-- | Unidirectional queue (output only)
newtype OutTQueue a = OutTQueue (TQueue a)

-- | Create a pair of unidirectional queues.
newSplitTQueueIO :: IO (InTQueue a, OutTQueue a)
newSplitTQueueIO = fmap splitTQueue newTQueueIO

-- | Split a TQueue into two unidirectional queues.
splitTQueue :: TQueue a -> (InTQueue a, OutTQueue a)
splitTQueue c = (InTQueue c, OutTQueue c)

writeOutTQueue :: OutTQueue a -> a -> STM ()
writeOutTQueue (OutTQueue c) a = writeTQueue c a

writeOutTQueueIO :: MonadIO m => OutTQueue a -> a -> m ()
writeOutTQueueIO q a = liftIO $ atomically $ writeOutTQueue q a

readInTQueue :: InTQueue a -> STM a
readInTQueue (InTQueue c) = readTQueue c

readInTQueueIO :: MonadIO m => InTQueue a -> m a
readInTQueueIO q = liftIO $ atomically $ readInTQueue q

-- | 'Resource' versions of string allocation and marshalling.
resourceNew :: Storable a => a -> Resource (Ptr a)
resourceNew a = mkResource (new a) free

rNew :: (Storable a, MonadResource m) => a -> m (Ptr a)
rNew a = fmap snd $ allocateResource (resourceNew a)

rMallocArray :: (Storable a, MonadResource m) => Int -> m (Ptr a)
rMallocArray sz = fmap snd $ allocateResource $ mkResource (mallocArray sz) free

rMalloc :: (Storable a, MonadResource m) => m (Ptr a)
rMalloc = fmap snd $ allocateResource $ mkResource malloc free

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
rNewCBString0 bs = do
  case isNulTerminated of
    True -> do
      (_, (_, cstr)) <- allocateResource $ mkResource alloc' free'
      return cstr
      where 
        alloc' = unsafeUseAsCString bs $ \cstr -> do
                  ptr <- newStablePtr cstr
                  return (ptr, cstr)
        free' (ptr, _) = freeStablePtr ptr
    False -> do
      (_, cstr) <- allocateResource $ mkResource alloc' free'
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
  (_, (_, cstrLen)) <- allocateResource $ mkResource alloc' free'
  return cstrLen
  where 
    alloc' = unsafeUseAsCStringLen bs $ \cstrLen -> do
              ptr <- newStablePtr cstrLen
              return (ptr, cstrLen)
    free' (ptr, _) = freeStablePtr ptr

-- | Run a ResourceT in an environment, capturing the resources it allocates and
-- registering them in a parent MonadResource. Return the release key for the 
-- captured resources, and the result of the action.
-- unwrapResourceT :: (MonadResource m, MonadBase IO m)
--                 => ResourceT IO a
--                 -> m (ReleaseKey, a)
unwrapResourceT (ResourceT r) = do
  -- TODO: run this past Michael Snoyman and see if I need to do anything more
  -- to mask exceptions
  -- P.S.: Apologies to Snoyman for putting down the wrong name.
  istate <- createInternalState
  rkey <- register (stateCleanup istate)
  result <- liftIO $ r istate
  return (rkey, result)

-- | Run a ResourceT in an environment, capturing the resources it allocates and
-- registering them in a parent MonadResource. Return the release key for the 
-- captured resources, and the result of the action.
--
-- This catches any exceptions from the input action, releasing resources and
-- returning 'Nothing' if an exception is caught.
tryUnwrapResourceT :: (MonadResource m, MonadBase IO m)
                   => ResourceT IO a
                   -> m (Maybe (ReleaseKey, a))
tryUnwrapResourceT (ResourceT r) = do
  istate <- createInternalState
  rkey <- register (stateCleanup istate)
  liftIO $ catch (runAction istate rkey) (handleException rkey) 
  where
    runAction istate rkey = do
      result <- r istate
      return $ Just (rkey, result)
    handleException rkey = \(_ :: SomeException) -> do
      release rkey
      return Nothing

forkIO_ :: IO a -> IO ()
forkIO_ = void . forkIO . void
{-# INLINE forkIO_ #-}
