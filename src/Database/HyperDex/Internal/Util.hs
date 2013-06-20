{-# LANGUAGE CPP #-}

module Database.HyperDex.Internal.Util
 ( wrapHyperCall
 , peekCBString, peekCBStringLen
 , newCBString, newCBStringLen
 , withCBString, withCBStringLen
 , withTextUtf8
 , newTextUtf8
 )
 where

import Foreign.C
import Data.ByteString.Char8
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)

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

-- | Marshal a NUL terminated C string into a ByteString.
--
peekCBString :: CString -> IO ByteString
peekCBString = fmap pack . peekCAString

-- | Marshal a C string with explicit length into a ByteString.
--
peekCBStringLen :: CStringLen -> IO ByteString
peekCBStringLen = fmap pack . peekCAStringLen

-- | Marshal a ByteString into a NUL terminated C string.
--
-- * the ByteString string /must not/ contain any NUL characters
--
-- * as with 'newCAString', new storage is allocated for the C String
--   and must be explicitly freed
--
newCBString :: ByteString -> IO CString
newCBString = newCAString . unpack

-- | Marshal a ByteString into a C string with explicit length.
-- 
-- * as with 'newCStringLen', new storage is allocated for the C String
--   and must be explicitly freed
--
newCBStringLen :: ByteString -> IO CStringLen
newCBStringLen = newCAStringLen . unpack

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
withCBString = withCAString . unpack

-- | Marshal a ByteString into a C string in temporary storage, 
-- with explicit length.
--
-- * the memory is freed when the subcomputation terminates (either
--   normally or via an exception), so the pointer to the temporary
--   storage /must not/ be used after this.
--
withCBStringLen :: ByteString -> (CStringLen -> IO a) -> IO a
withCBStringLen = withCAStringLen . unpack

-- | Marshal a Text field as a UTF8 C string in temporary storage.
-- 
-- * the memory is freed when the subcomputation terminates (either
--   normally or via an exception), so the pointer to the temporary
--   storage /must not/ be used after this.
--
withTextUtf8 :: Text -> (CString -> IO a) -> IO a
withTextUtf8 = withCBString . encodeUtf8

-- | Marshal a Text field as a UTF8 C string.
-- 
-- * the Text input /must not/ contain any NUL characters
--
-- * as with 'newCAString', new storage is allocated for the C String
--   and must be explicitly freed
--
newTextUtf8 :: Text -> IO CString
newTextUtf8 = newCBString . encodeUtf8
