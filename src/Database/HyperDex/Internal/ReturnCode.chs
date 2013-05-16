module Database.HyperDex.Internal.ReturnCode 
  ( ReturnCode (..) ) 
  where

#include "hyperclient.h"

{#enum hyperclient_returncode as ReturnCode {underscoreToCase} deriving (Eq, Show) #}
