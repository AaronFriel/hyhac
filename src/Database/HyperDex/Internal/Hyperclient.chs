module Database.HyperDex.Internal.Hyperclient where

import Foreign.C.Types
import Foreign.C.String
import Foreign.Marshal.Utils
import Foreign.Ptr
import Foreign.Storable
import Data.Int

#import "hyperclient.h"

data Hyperclient
{#pointer *hyperclient as HyperclientPtr -> Hyperclient #}

data HyperclientMapAttribute
{#pointer *hyperclient_map_attribute as HyperclientMapAttributePtr -> HyperclientMapAttribute #}

data HyperclientAttributeCheck
{#pointer *hyperclient_attribute_check as HyperclientAttributeCheckPtr -> HyperclientAttributeCheck #}

{#enum hyperclient_returncode as HcReturnCode {underscoreToCase} #}

{#fun hyperclient_create as ^
  { `String', `Int16' } -> `HyperclientPtr' id #}
