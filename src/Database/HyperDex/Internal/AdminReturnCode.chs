
-- |
-- Module     	: Database.HyperDex.Internal.AdminReturnCode
-- Copyright  	: (c) Aaron Friel 2013-2014
--            	  (c) Niklas Hambüchen 2013-2014 
-- License    	: BSD-style
-- Maintainer 	: mayreply@aaronfriel.com
-- Stability  	: unstable
-- Portability	: portable
--
module Database.HyperDex.Internal.AdminReturnCode
  ( ReturnCode (..) )
  where

#include "hyperdex/admin.h"

{#enum hyperdex_admin_returncode as ReturnCode {underscoreToCase} deriving (Eq, Show) #}
