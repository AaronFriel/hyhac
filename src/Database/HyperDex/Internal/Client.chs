{-# LANGUAGE TypeFamilies, FlexibleInstances, TypeSynonymInstances #-}

-- |
-- Module       : Database.HyperDex.Internal.Client
-- Copyright    : (c) Aaron Friel 2013-2014
--                (c) Niklas Hambüchen 2013-2014
-- License      : BSD-style
-- Maintainer   : mayreply@aaronfriel.com
-- Stability    : unstable
-- Portability  : portable
--
module Database.HyperDex.Internal.Client
  ( Client
  , ClientConnection
  , ClientReturnCode
  , ClientResult
  , ReturnCode (..)
  , clientConnect
  , peekReturnCode
  , clientDeferred
  , clientIterator
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

#include "hyperdex/client.h"

data OpaqueClient
{#pointer *hyperdex_client as Client -> OpaqueClient #}

type ClientConnection = HyperDexConnection Client

type ClientReturnCode = ReturnCode Client

type ClientResult a = AsyncResult Client a

peekReturnCode :: MonadIO m 
               => Ptr CInt
               -> m (ReturnCode Client)
peekReturnCode = liftIO . fmap (toEnum . fromIntegral) . peek

instance HyperDex Client where
  data ReturnCode Client = ClientSuccess
                             | ClientNotfound
                             | ClientSearchdone
                             | ClientCmpfail
                             | ClientReadonly
                             | ClientUnknownspace
                             | ClientCoordfail
                             | ClientServererror
                             | ClientPollfailed
                             | ClientOverflow
                             | ClientReconfigure
                             | ClientTimeout
                             | ClientUnknownattr
                             | ClientDupeattr
                             | ClientNonepending
                             | ClientDontusekey
                             | ClientWrongtype
                             | ClientNomem
                             | ClientBadconfig
                             | ClientDuplicate
                             | ClientInterrupted
                             | ClientClusterJump
                             | ClientCoordLogged
                             | ClientOffline
                             | ClientInternal
                             | ClientException
                             | ClientGarbage
                             deriving (Show,Eq)


  failureCode = ClientGarbage

  isTransient ClientInterrupted = True
  isTransient ClientTimeout     = True
  isTransient _                 = False 
  
  isGlobalError ClientInternal  = True
  isGlobalError ClientException = True
  isGlobalError ClientGarbage   = True
  isGlobalError _               = False 

  isNonePending ClientNonepending = True
  isNonePending _                 = False

  create host port =
    wrapHyperCall $ {# call hyperdex_client_create #} host port

  destroy ptr =
    wrapHyperCall $ {# call hyperdex_client_destroy #} ptr

  loop ptr timeout =
    alloca $ \returnCodePtr -> do
      handle <- wrapHyperCall $ {# call hyperdex_client_loop #} ptr timeout returnCodePtr
      returnCode <- peekReturnCode returnCodePtr
      return (mkHandle handle, returnCode)

instance Enum (ReturnCode Client) where
  fromEnum ClientSuccess = 8448
  fromEnum ClientNotfound = 8449
  fromEnum ClientSearchdone = 8450
  fromEnum ClientCmpfail = 8451
  fromEnum ClientReadonly = 8452
  fromEnum ClientUnknownspace = 8512
  fromEnum ClientCoordfail = 8513
  fromEnum ClientServererror = 8514
  fromEnum ClientPollfailed = 8515
  fromEnum ClientOverflow = 8516
  fromEnum ClientReconfigure = 8517
  fromEnum ClientTimeout = 8519
  fromEnum ClientUnknownattr = 8520
  fromEnum ClientDupeattr = 8521
  fromEnum ClientNonepending = 8523
  fromEnum ClientDontusekey = 8524
  fromEnum ClientWrongtype = 8525
  fromEnum ClientNomem = 8526
  fromEnum ClientBadconfig = 8527
  fromEnum ClientDuplicate = 8529
  fromEnum ClientInterrupted = 8530
  fromEnum ClientClusterJump = 8531
  fromEnum ClientCoordLogged = 8532
  fromEnum ClientOffline = 8533
  fromEnum ClientInternal = 8573
  fromEnum ClientException = 8574
  fromEnum ClientGarbage = 8575

  toEnum 8448 = ClientSuccess
  toEnum 8449 = ClientNotfound
  toEnum 8450 = ClientSearchdone
  toEnum 8451 = ClientCmpfail
  toEnum 8452 = ClientReadonly
  toEnum 8512 = ClientUnknownspace
  toEnum 8513 = ClientCoordfail
  toEnum 8514 = ClientServererror
  toEnum 8515 = ClientPollfailed
  toEnum 8516 = ClientOverflow
  toEnum 8517 = ClientReconfigure
  toEnum 8519 = ClientTimeout
  toEnum 8520 = ClientUnknownattr
  toEnum 8521 = ClientDupeattr
  toEnum 8523 = ClientNonepending
  toEnum 8524 = ClientDontusekey
  toEnum 8525 = ClientWrongtype
  toEnum 8526 = ClientNomem
  toEnum 8527 = ClientBadconfig
  toEnum 8529 = ClientDuplicate
  toEnum 8530 = ClientInterrupted
  toEnum 8531 = ClientClusterJump
  toEnum 8532 = ClientCoordLogged
  toEnum 8533 = ClientOffline
  toEnum 8573 = ClientInternal
  toEnum 8574 = ClientException
  toEnum 8575 = ClientGarbage
  toEnum unmatched = error ("ClientReturnCode.toEnum: Cannot match " ++ show unmatched)

clientConnect :: ConnectInfo -> IO (ClientConnection)
clientConnect = connect

clientDeferred :: ResIO (AsyncCall Client a)
               -> ClientConnection
               -> IO (AsyncResult Client a)
clientDeferred = wrapDeferred (ClientSuccess==)

clientIterator :: ResIO (AsyncCall Client a)
               -> ClientConnection
               -> IO (Stream Client a)
clientIterator = wrapIterator (ClientSearchdone==) (ClientSuccess==)

-- clientDisconnect :: (ClientConnection) -> IO ()
-- clientDisconnect = disconnect
