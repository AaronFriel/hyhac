module Database.HyperDex.Internal.AdminReturnCode
  ( ReturnCode (..) )
  where

#include "hyperdex/admin.h"

{#enum hyperdex_admin_returncode as ReturnCode {underscoreToCase} deriving (Eq, Show) #}
