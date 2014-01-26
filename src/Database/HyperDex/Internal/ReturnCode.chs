
-- |
-- Module     	: Database.HyperDex.Internal.ReturnCode
-- Copyright  	: (c) Aaron Friel 2013-2014
-- License    	: BSD-style
-- Maintainer 	: mayreply@aaronfriel.com
-- Stability  	: unstable
-- Portability	: portable
--
module Database.HyperDex.Internal.ReturnCode
  ( ReturnCode (..) )
  where

#include "hyperdex/client.h"

{#enum hyperdex_client_returncode as ReturnCode {underscoreToCase} deriving (Eq, Show) #}
