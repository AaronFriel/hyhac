module Database.HyperDex.Internal.ReturnCode 
  ( HyperclientReturnCode (..) ) 
  where

#include "hyperclient.h"

{#enum hyperclient_returncode as HyperclientReturnCode {underscoreToCase} deriving (Eq, Show) #}
