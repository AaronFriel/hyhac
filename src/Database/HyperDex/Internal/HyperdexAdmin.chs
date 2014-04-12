
-- |
-- Module       : Database.HyperDex.Internal.HyperdexAdmin
-- Copyright    : (c) Aaron Friel 2013-2014
-- License      : BSD-style
-- Maintainer   : mayreply@aaronfriel.com
-- Stability    : unstable
-- Portability  : portable
--
module Database.HyperDex.Internal.HyperdexAdmin
  ( dumpConfig
  , addSpace
  , rmSpace
  , setReadOnly
  , waitUntilStable
  , validateSpace
  , listSpaces
  , serverRegister
  , serverOnline
  , serverOffline
  , serverForget
  , serverKill
  , enablePerfCounters
  , disablePerfCounters
  , rawBackup
  )
  where

import Foreign
import Foreign.C

import Data.ByteString (packCString)
import qualified Data.ByteString.Char8 as BS

import Control.Monad.IO.Class

#include "hyperdex/admin.h"

{# import Database.HyperDex.Internal.Admin #}
{# import Database.HyperDex.Internal.PerfCounter #}
import Database.HyperDex.Internal.Core
import Database.HyperDex.Internal.Handle (wrapHyperCallHandle)
import Database.HyperDex.Internal.Util

-- int64_t
-- hyperdex_admin_dump_config(struct hyperdex_admin* admin,
--                            enum hyperdex_admin_returncode* status,
--                            const char** config);
dumpConfig = adminDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ AdminGarbage)
  configPtr <- rMalloc
  let ccall ptr = 
        wrapHyperCallHandle $
          {# call hyperdex_admin_dump_config #}
            ptr
            returnCodePtr
            configPtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          AdminSuccess -> liftIO $ do
                            cStr <- peek configPtr
                            config <- packCString cStr
                            return $ Right config
          _            -> return $ Left returnCode
  return $ AsyncCall ccall callback

-- int64_t
-- hyperdex_admin_read_only(struct hyperdex_admin* admin, int ro,
--                          enum hyperdex_admin_returncode* status);
setReadOnly ro = adminDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ AdminGarbage)
  let ccall ptr = 
        wrapHyperCallHandle $
          {# call hyperdex_admin_read_only #}
            ptr
            (if ro then 1 else 0)
            returnCodePtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          AdminSuccess -> return $ Right ()
          _            -> return $ Left returnCode
  return $ AsyncCall ccall callback

-- int64_t
-- hyperdex_admin_wait_until_stable(struct hyperdex_admin* admin,
--                                  enum hyperdex_admin_returncode* status);
waitUntilStable = adminDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ AdminGarbage)
  let ccall ptr = 
        wrapHyperCallHandle $
          {# call hyperdex_admin_wait_until_stable #}
            ptr
            returnCodePtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          AdminSuccess -> return $ Right ()
          _            -> return $ Left returnCode
  return $ AsyncCall ccall callback

-- int
-- hyperdex_admin_validate_space(struct hyperdex_admin* admin,
--                               const char* description,
--                               enum hyperdex_admin_returncode* status);
validateSpace desc = adminImmediate $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ AdminGarbage)
  descPtr <- rNewCBString0 desc
  let ccall ptr = 
        wrapHyperCall $
          {# call hyperdex_admin_validate_space #}
            ptr
            descPtr
            returnCodePtr
  let callback r = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          AdminSuccess -> return $ Right r
          _            -> return $ Left returnCode
  return $ SyncCall ccall callback

-- int64_t
-- hyperdex_admin_add_space(struct hyperdex_admin* admin,
--                          const char* description,
--                          enum hyperdex_admin_returncode* status);
addSpace desc = adminDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ AdminGarbage)
  descPtr <- rNewCBString0 desc
  let ccall ptr = 
        wrapHyperCallHandle $
          {# call hyperdex_admin_add_space #}
            ptr
            descPtr
            returnCodePtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          AdminSuccess -> return $ Right ()
          _            -> return $ Left returnCode
  return $ AsyncCall ccall callback

-- int64_t
-- hyperdex_admin_rm_space(struct hyperdex_admin* admin,
--                         const char* name,
--                         enum hyperdex_admin_returncode* status);
rmSpace s = adminDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ AdminGarbage)
  space <- rNewCBString0 s
  let ccall ptr = 
        wrapHyperCallHandle $
          {# call hyperdex_admin_rm_space #}
            ptr
            space
            returnCodePtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          AdminSuccess -> return $ Right ()
          _            -> return $ Left returnCode
  return $ AsyncCall ccall callback

-- int64_t
-- hyperdex_admin_list_spaces(struct hyperdex_admin* admin,
--                            enum hyperdex_admin_returncode* status,
--                            const char** spaces);
listSpaces = adminDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ AdminGarbage)
  spacesPtr <- rMalloc
  let ccall ptr = 
        wrapHyperCallHandle $
          {# call hyperdex_admin_list_spaces #}
            ptr
            returnCodePtr
            spacesPtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          AdminSuccess -> liftIO $ do
                            cStr <- peek spacesPtr
                            config <- packCString cStr
                            return $ Right $ BS.lines config
          _            -> return $ Left returnCode
  return $ AsyncCall ccall callback

-- int64_t
-- hyperdex_admin_server_register(struct hyperdex_admin* admin,
--                                uint64_t token, const char* address,
--                                enum hyperdex_admin_returncode* status);
serverRegister t h = adminDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ AdminGarbage)
  host <- rNewCBString0 h
  let ccall ptr = 
        wrapHyperCallHandle $
          {# call hyperdex_admin_server_register #}
            ptr
            t
            host
            returnCodePtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          AdminSuccess -> return $ Right ()
          _            -> return $ Left returnCode
  return $ AsyncCall ccall callback

-- int64_t
-- hyperdex_admin_server_online(struct hyperdex_admin* admin,
--                              uint64_t token,
--                              enum hyperdex_admin_returncode* status);
serverOnline = serverOp {# call hyperdex_admin_server_online #}

-- int64_t
-- hyperdex_admin_server_offline(struct hyperdex_admin* admin,
--                               uint64_t token,
--                               enum hyperdex_admin_returncode* status);
serverOffline = serverOp {# call hyperdex_admin_server_offline #}

-- int64_t
-- hyperdex_admin_server_forget(struct hyperdex_admin* admin,
--                              uint64_t token,
--                              enum hyperdex_admin_returncode* status);
serverForget = serverOp {# call hyperdex_admin_server_forget #}

-- int64_t
-- hyperdex_admin_server_kill(struct hyperdex_admin* admin,
--                            uint64_t token,
--                            enum hyperdex_admin_returncode* status);
serverKill = serverOp {# call hyperdex_admin_server_forget #}

-- int64_t
-- hyperdex_admin_enable_perf_counters(struct hyperdex_admin* admin,
--                                     enum hyperdex_admin_returncode* status,
--                                     struct hyperdex_admin_perf_counter* pc);
-- TODO: perf counters object
enablePerfCounters = wrapIterator (AdminSuccess==) (const False) $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ AdminGarbage)
  perfCounterPtr <- rMalloc
  let ccall ptr =
        wrapHyperCallHandle $
          {# call hyperdex_admin_enable_perf_counters #}
            ptr
            returnCodePtr
            perfCounterPtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          AdminSuccess -> liftIO $ fmap Right $ peek perfCounterPtr
          _            -> return $ Left returnCode
  return $ AsyncCall ccall callback

-- void
-- hyperdex_admin_disable_perf_counters(struct hyperdex_admin* admin);
disablePerfCounters = adminImmediate $ do
  let ccall ptr = 
        wrapHyperCall $
          {# call hyperdex_admin_disable_perf_counters #}
            ptr
  let callback () = return $ Right ()
  return $ SyncCall ccall callback

-- int
-- hyperdex_admin_raw_backup(const char* host, uint16_t port,
--                           const char* name,
--                           enum hyperdex_admin_returncode* status);
-- TODO: update to latest API for backup
rawBackup host port name = adminImmediate $ do
  let portVal = CUShort $ port
  hostPtr <- rNewCBString0 $ host
  namePtr <- rNewCBString0 $ name
  returnCodePtr <- rNew (fromIntegral . fromEnum $ AdminGarbage)
  let ccall _ = 
        wrapHyperCall $
          {# call hyperdex_admin_raw_backup #}
            hostPtr portVal
            namePtr
            returnCodePtr
  let callback r = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          AdminSuccess -> return $ Right r
          _            -> return $ Left returnCode
  return $ SyncCall ccall callback


serverOp call t = adminDeferred $ do
  returnCodePtr <- rNew (fromIntegral . fromEnum $ AdminGarbage)
  let ccall ptr = 
        wrapHyperCallHandle $
          call
            ptr
            t
            returnCodePtr
  let callback = do
        returnCode <- peekReturnCode returnCodePtr
        case returnCode of
          AdminSuccess -> return $ Right ()
          _            -> return $ Left returnCode
  return $ AsyncCall ccall callback
