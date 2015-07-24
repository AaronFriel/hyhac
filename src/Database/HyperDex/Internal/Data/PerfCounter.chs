-- |
-- Module       : Database.HyperDex.Internal.PerfCounter
-- Copyright    : (c) Aaron Friel 2013-2014
--                (c) Niklas Hambüchen 2013-2014 
-- License      : BSD-style
-- Maintainer   : mayreply@aaronfriel.com
-- Stability    : unstable
-- Portability  : portable
--
module Database.HyperDex.Internal.Data.PerfCounter
  ( PerfCounter (..)
  , PerfCounterPtr
  )
  where

import Foreign
import Foreign.C

import Data.ByteString (ByteString, packCString)

import Database.HyperDex.Internal.Util

import Control.Monad
import Control.Applicative ((<$>), (<*>))

#include "hyperdex/admin.h"

#c
typedef struct hyperdex_admin_perf_counter hyperdex_admin_perf_counter_struct;
#endc

{# pointer *hyperdex_admin_perf_counter as PerfCounterPtr -> PerfCounter #}

data PerfCounter = PerfCounter
  { ctrName        :: Word64
  , ctrTime        :: Word64
  , ctrProperty    :: ByteString
  , ctrMeasurement :: Word64
  }
  deriving (Show, Eq, Ord)
instance Storable PerfCounter where
  sizeOf _ = {#sizeof hyperdex_admin_perf_counter_struct #}
  alignment _ = {#alignof hyperdex_admin_perf_counter_struct #}
  peek p = PerfCounter
    <$> liftM (fromIntegral) ({#get hyperdex_admin_perf_counter.id #} p)
    <*> liftM (fromIntegral) ({#get hyperdex_admin_perf_counter.time #} p)
    <*> (packCString =<< ({#get hyperdex_admin_perf_counter.property #} p))
    <*> liftM (fromIntegral) ({#get hyperdex_admin_perf_counter.measurement #} p)
  poke p x = do
    prop <- newCBString (ctrProperty x)
    {#set hyperdex_admin_perf_counter.id #} p $ fromIntegral $ ctrName x
    {#set hyperdex_admin_perf_counter.time #} p $ fromIntegral $ ctrTime x
    {#set hyperdex_admin_perf_counter.property #} p prop
    {#set hyperdex_admin_perf_counter.measurement #} p $ fromIntegral $ ctrMeasurement x
