{-# LANGUAGE CPP #-}

module Database.HyperDex.Internal.Util
 ( wrapHyperCall
 , newCBString, newCBStringLen
 , withTextUtf8
 , newTextUtf8
 , peekTextUtf8
 )
 where

import Foreign
import Foreign.C
import Data.ByteString.Char8
import Data.ByteString.Unsafe (unsafeUseAsCString, unsafeUseAsCStringLen)
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8, decodeUtf8)

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
                     let l = Data.ByteString.Char8.length bs
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
withTextUtf8 = useAsCString . encodeUtf8
{-# INLINE withTextUtf8 #-}

-- | Marshal a Text field as a UTF8 C string.
--
-- * the Text input /must not/ contain any NUL characters
--
-- * as with 'newCAString', new storage is allocated for the C String
--   and must be explicitly freed
--
newTextUtf8 :: Text -> IO CString
newTextUtf8 = newCBString . encodeUtf8
{-# INLINE newTextUtf8 #-}

peekTextUtf8 :: Ptr CString -> IO Text
peekTextUtf8 ptr = do
  bstring <- peek ptr >>= packCString
  return $ decodeUtf8 bstring 
{-# INLINE peekTextUtf8 #-} 
