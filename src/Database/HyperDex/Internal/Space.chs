
-- |
-- Module     	: Database.HyperDex.Internal.Space
-- Copyright  	: (c) Aaron Friel 2013-2014
--            	  (c) Niklas Hambüchen 2013-2014 
-- License    	: BSD-style
-- Maintainer 	: mayreply@aaronfriel.com
-- Stability  	: unstable
-- Portability	: portable
--
module Database.HyperDex.Internal.Space
  ( addSpace
  , removeSpace
  )
  where

import Foreign
import Foreign.C

import Data.Text (Text)

{#import Database.HyperDex.Internal.Admin #}
{#import Database.HyperDex.Internal.AdminReturnCode #}
import Database.HyperDex.Internal.Util

#include "hyperdex/admin.h"

addSpace :: Admin -> Text -> IO ReturnCode
addSpace c desc  = withAdminImmediate c $ \hc -> do
  hyperdexAdminAddSpace hc desc

removeSpace :: Admin -> Text -> IO ReturnCode
removeSpace c name = withAdminImmediate c $ \hc -> do
  hyperdexAdminRemoveSpace hc name

-- int64_t
-- hyperdex_admin_add_space(struct hyperdex_admin* admin, const char* description, enum hyperdex_admin_returncode* status);
hyperdexAdminAddSpace :: HyperdexAdmin -> Text -> IO ReturnCode
hyperdexAdminAddSpace admin d = withTextUtf8 d $ \description -> do
  alloca $ \returnCodePtr -> do
    -- TODO nh2: handle int64_t return (and figure out what it is)
    _ <- wrapHyperCall $ {#call hyperdex_admin_add_space #} admin description returnCodePtr
    returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
    return returnCode

-- int64_t
-- hyperdex_admin_rm_space(struct hyperdex_admin* admin, const char* name, enum hyperdex_admin_returncode* status);
hyperdexAdminRemoveSpace :: HyperdexAdmin -> Text -> IO ReturnCode
hyperdexAdminRemoveSpace admin s = withTextUtf8 s $ \space -> do
  alloca $ \returnCodePtr -> do
    -- TODO nh2: handle int64_t return (and figure out what it is)
    _ <- wrapHyperCall $ {#call hyperdex_admin_rm_space #} admin space returnCodePtr
    returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
    return returnCode
