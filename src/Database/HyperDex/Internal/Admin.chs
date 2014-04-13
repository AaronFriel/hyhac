{-# LANGUAGE TypeFamilies, FlexibleInstances, TypeSynonymInstances #-}

-- |
-- Module     	: Database.HyperDex.Internal.Admin
-- Copyright  	: (c) Aaron Friel 2013-2014
--            	  (c) Niklas Hambüchen 2013-2014 
-- License    	: BSD-style
-- Maintainer 	: mayreply@aaronfriel.com
-- Stability  	: unstable
-- Portability	: portable
--
module Database.HyperDex.Internal.Admin
  ( Admin
  , ReturnCode (..)
  , adminConnect
  , peekReturnCode
  , adminDeferred
  , adminImmediate
  )
  where

import Database.HyperDex.Internal.Core
import Database.HyperDex.Internal.Handle
import Database.HyperDex.Internal.Options
import Database.HyperDex.Internal.Util

import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import Foreign
import Foreign.C

#include "hyperdex/admin.h"

data OpaqueAdmin
{#pointer *hyperdex_admin as Admin -> OpaqueAdmin #}

peekReturnCode :: MonadIO m 
               => Ptr CInt
               -> m (ReturnCode Admin)
peekReturnCode = liftIO . fmap (toEnum . fromIntegral) . peek

instance HyperDex Admin where
  data ReturnCode Admin = AdminSuccess
                        | AdminNomem
                        | AdminNonepending
                        | AdminPollfailed
                        | AdminTimeout
                        | AdminInterrupted
                        | AdminServererror
                        | AdminCoordfail
                        | AdminBadspace
                        | AdminDuplicate
                        | AdminNotfound
                        | AdminInternal
                        | AdminException
                        | AdminGarbage
                        deriving (Eq,Show)

  failureCode = AdminSuccess

  isTransient AdminInterrupted = True
  isTransient _                = False 
  
  isGlobalError AdminInternal  = True
  isGlobalError AdminException = True
  isGlobalError AdminGarbage   = True
  isGlobalError _              = False 

  isNonePending AdminNonepending = True
  isNonePending _                = False

  create host port =
    wrapHyperCall $ {# call hyperdex_admin_create #} host port

  destroy ptr =
    wrapHyperCall $ {# call hyperdex_admin_destroy #} ptr

  loop ptr timeout =
    alloca $ \returnCodePtr -> do
      handle <- wrapHyperCall $ {# call hyperdex_admin_loop #} ptr timeout returnCodePtr
      returnCode <- peekReturnCode returnCodePtr
      return (mkHandle handle, returnCode)

instance Enum (ReturnCode Admin) where
  fromEnum AdminSuccess = 8704
  fromEnum AdminNomem = 8768
  fromEnum AdminNonepending = 8769
  fromEnum AdminPollfailed = 8770
  fromEnum AdminTimeout = 8771
  fromEnum AdminInterrupted = 8772
  fromEnum AdminServererror = 8773
  fromEnum AdminCoordfail = 8774
  fromEnum AdminBadspace = 8775
  fromEnum AdminDuplicate = 8776
  fromEnum AdminNotfound = 8777
  fromEnum AdminInternal = 8829
  fromEnum AdminException = 8830
  fromEnum AdminGarbage = 8831

  toEnum 8704 = AdminSuccess
  toEnum 8768 = AdminNomem
  toEnum 8769 = AdminNonepending
  toEnum 8770 = AdminPollfailed
  toEnum 8771 = AdminTimeout
  toEnum 8772 = AdminInterrupted
  toEnum 8773 = AdminServererror
  toEnum 8774 = AdminCoordfail
  toEnum 8775 = AdminBadspace
  toEnum 8776 = AdminDuplicate
  toEnum 8777 = AdminNotfound
  toEnum 8829 = AdminInternal
  toEnum 8830 = AdminException
  toEnum 8831 = AdminGarbage
  toEnum unmatched = error ("AdminReturnCode.toEnum: Cannot match " ++ show unmatched)

adminConnect :: ConnectInfo -> IO (HyperDexConnection Admin)
adminConnect = connect

adminDeferred :: ResIO (AsyncCall Admin a) 
              -> HyperDexConnection Admin
              -> IO (AsyncResult Admin a)
adminDeferred = wrapDeferred (AdminSuccess==)

adminImmediate :: ResIO (SyncCall Admin a)
               -> HyperDexConnection Admin
               -> IO (AsyncResult Admin a)
adminImmediate = wrapImmediate

-- adminDisconnect :: (HyperDexConnection Admin) -> IO ()
-- adminDisconnect = disconnect
