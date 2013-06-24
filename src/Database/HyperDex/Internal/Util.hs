{-# LANGUAGE CPP #-}

module Database.HyperDex.Internal.Util
 ( wrapHyperCall
 , peekCBString, peekCBStringLen
 , newCBString, newCBStringLen
 , withCBString, withCBStringLen
 , withTextUtf8
 , newTextUtf8
 , peekTextUtf8
 )
 where

import Foreign
import Foreign.C
import Data.ByteString.Char8
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

-- | Marshal a NUL terminated C string into a ByteString.
--
peekCBString :: CString -> IO ByteString
peekCBString = fmap pack . peekCAString
{-# INLINE peekCBString #-}

-- | Marshal a C string with explicit length into a ByteString.
--
peekCBStringLen :: CStringLen -> IO ByteString
peekCBStringLen = fmap pack . peekCAStringLen
{-# INLINE peekCBStringLen #-}

-- | Marshal a ByteString into a NUL terminated C string.
--
-- * the ByteString string /must not/ contain any NUL characters
--
-- * as with 'newCAString', new storage is allocated for the C String
--   and must be explicitly freed
--
newCBString :: ByteString -> IO CString
newCBString bs = useAsCString bs
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
newCBStringLen bs = useAsCStringLen bs
                    (\(cs,l) -> do
                      buf <- mallocArray l
                      copyBytes buf cs l
                      return (buf,l))
{-# INLINE newCBStringLen #-}

-- | Marshal a ByteString into a NUL terminated C string using temporary
-- storage.
--
-- * the ByteString string /must not/ contain any NUL characters
--
-- * the memory is freed when the subcomputation terminates (either
--   normally or via an exception), so the pointer to the temporary
--   storage /must not/ be used after this.
--
withCBString :: ByteString -> (CString -> IO a) -> IO a
withCBString = useAsCString
{-# INLINE withCBString #-}

-- | Marshal a ByteString into a C string in temporary storage,
-- with explicit length.
--
-- * the memory is freed when the subcomputation terminates (either
--   normally or via an exception), so the pointer to the temporary
--   storage /must not/ be used after this.
--
withCBStringLen :: ByteString -> (CStringLen -> IO a) -> IO a
withCBStringLen = useAsCStringLen
{-# INLINE withCBStringLen #-}

-- | Marshal a Text field as a UTF8 C string in temporary storage.
--
-- * the memory is freed when the subcomputation terminates (either
--   normally or via an exception), so the pointer to the temporary
--   storage /must not/ be used after this.
--
withTextUtf8 :: Text -> (CString -> IO a) -> IO a
withTextUtf8 = withCBString . encodeUtf8
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
