{-# LANGUAGE CPP, ScopedTypeVariables, FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
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
 , whenDebug
 , traceIO
 , traceByteString
 , traceCStringLen
 , traceIndirectCStringLen
 , traceAsCStringLiteral
 )
 where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.ByteString.Unsafe (unsafeUseAsCString, unsafeUseAsCStringLen, unsafePackCStringLen)
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import Foreign hiding (void)
import Foreign.C

import GHC.IO.Handle
import GHC.IO.Handle.FD
import Data.Attoparsec.ByteString hiding (word8)
import Data.ByteString.Builder
import Data.Monoid
import Control.Applicative ((<|>))

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

whenDebug :: Monad m => m () -> m ()
#ifdef DEBUG
whenDebug f = f
#else
whenDebug _ = return ()
#endif

traceHandle :: Handle
traceHandle = stderr 
traceIO :: MonadIO m => String -> m ()
traceIO = whenDebug . liftIO . hPutStr traceHandle

traceByteString :: MonadIO m => ByteString -> m ()
traceByteString bs = whenDebug $ 
  liftIO $ hPutBuilder traceHandle (byteStringHex bs)

traceCStringLen :: forall a m. MonadIO m => Storable a => (Ptr a, Int) -> m ()
traceCStringLen (ptr, sz) = whenDebug $
  liftIO (unsafePackCStringLen (castPtr ptr, sz * sizeOfStorable)) >>= traceByteString
  where sizeOfStorable = sizeOf (undefined :: a)  

traceIndirectCStringLen :: MonadIO m => Storable a => (Ptr (Ptr a), Ptr CULong) -> m ()
traceIndirectCStringLen (ptrPtr, ptrSz) = whenDebug $ do
  cstr <- liftIO $ do
    ptr <- peek ptrPtr
    sz <- peek ptrSz
    return (ptr, fromIntegral sz)
  traceCStringLen cstr

printableChar :: Word8 -> Bool
printableChar c = 
  case c of
    0x07 -> False
    0x08 -> False
    0x0a -> False
    0x0b -> False
    0x0c -> False
    0x0d -> False
    0x22 -> False
    0x27 -> False
    0x3f -> False
    0x5c -> False
    _    -> c >= 0x20 && c < 0x80

hex :: Word8 -> Char
hex 0  = '0'
hex 1  = '1'
hex 2  = '2'
hex 3  = '3'
hex 4  = '4'
hex 5  = '5'
hex 6  = '6'
hex 7  = '7'
hex 8  = '8'
hex 9  = '9'
hex 10 = 'a'
hex 11 = 'b'
hex 12 = 'c'
hex 13 = 'd'
hex 14 = 'e'
hex 15 = 'f'
hex _  = '\0'

parseNonPrintableChar :: Parser Builder
parseNonPrintableChar = do
  char <- anyWord8
  let (high, low) = char `divMod` 16
  return $ byteString "\\x" <> char7 (hex high) <> char7 (hex low)
            <> byteString "\"\""

parsePrintableChars :: Parser Builder
parsePrintableChars = do
  str <- takeWhile1 printableChar
  return $ byteString str

parseStringLiteral :: Parser Builder
parseStringLiteral = do
  strs <- many1 (parsePrintableChars <|> parseNonPrintableChar)
  return $ mconcat strs

parseAsCStringLiteral :: ByteString -> Either String Builder
parseAsCStringLiteral bs = 
  case parseOnly parseStringLiteral bs of 
    Left e        -> Left e
    Right builder -> Right $ char7 '\"' <> builder <> char7 '\"'

traceAsCStringLiteral :: MonadIO m => ByteString -> m ()
traceAsCStringLiteral bs = whenDebug $
  case parseAsCStringLiteral bs of
    Left  _ -> liftIO $ hPutStr traceHandle "\"\""
    Right builder -> liftIO $ hPutBuilder traceHandle builder
