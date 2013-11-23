module Database.HyperDex.Internal.ReturnCode 
  ( ReturnCode (..) ) 
  where

#include "hyperdex/client.h"

{#enum hyperdex_client_returncode as ReturnCode {underscoreToCase} deriving (Eq, Show) #}
