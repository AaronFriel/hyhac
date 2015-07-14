
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

addSpace :: Admin -> Text -> AsyncResult ()
addSpace client d = withAdmin client $ \hyperdexClient -> do
  description <- newTextUtf8 d
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperdexAdminGarbage)
  handle <- wrapHyperCall $ 
              {# call safe hyperdex_admin_add_space #}
              hyperdexClient description returnCodePtr
  let continuation = do
        returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
        free returnCodePtr
        free description
        return $ 
          case returnCode of
            HyperdexAdminSuccess -> Right ()
            _                    -> Left returnCode
  return (handle, continuation)

removeSpace :: Admin -> Text -> AsyncResult ()
removeSpace client s = withAdmin client $ \hyperdexClient -> do
  space <- newTextUtf8 s
  returnCodePtr <- new (fromIntegral . fromEnum $ HyperdexAdminGarbage)
  handle <- wrapHyperCall $ 
              {# call safe hyperdex_admin_rm_space #}
              hyperdexClient space returnCodePtr
  let continuation = do
        returnCode <- fmap (toEnum . fromIntegral) $ peek returnCodePtr
        free returnCodePtr
        free space
        return $ 
          case returnCode of
            HyperdexAdminSuccess -> Right ()
            _                    -> Left returnCode
  return (handle, continuation)
