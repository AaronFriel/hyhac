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
 -- Miscellany
 , forkIO_
 , unCULong
 )
 where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class
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

forkIO_ :: IO a -> IO ()
forkIO_ = void . forkIO . void
{-# INLINE forkIO_ #-}

unCULong :: CULong -> Word64
unCULong (CULong w) = w
