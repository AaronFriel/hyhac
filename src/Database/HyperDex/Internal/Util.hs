module Database.HyperDex.Internal.Util
 ( AllocBy (..)
 , peekCBString, peekCBStringLen
 , newCBString, newCBStringLen
 , withCBString, withCBStringLen
 )
 where

import Data.ByteString.Char8
import Foreign.C.String

-- | Indicates whether an object or array has been allocated 
-- by Haskell or by the HyperDex library.
data AllocBy = AllocHaskell | AllocHyperDex

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
