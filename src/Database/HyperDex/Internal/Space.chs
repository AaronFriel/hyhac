module Database.HyperDex.Internal.Space 
  ( addSpace
  , removeSpace
  )
  where

import Foreign
import Foreign.C

import Data.Text (Text)

{#import Database.HyperDex.Internal.Client #}
{#import Database.HyperDex.Internal.ReturnCode #}
import Database.HyperDex.Internal.Util

#include "hyperclient.h"

addSpace :: Client -> Text -> IO ReturnCode
addSpace c desc  = withClientImmediate c $ \hc -> do
  hyperclientAddSpace hc desc

removeSpace :: Client -> Text -> IO ReturnCode
removeSpace c name = withClientImmediate c $ \hc -> do
  hyperclientRemoveSpace hc name

-- enum hyperclient_returncode
-- hyperclient_add_space(struct hyperclient* client, const char* description);
hyperclientAddSpace :: Hyperclient -> Text -> IO ReturnCode
hyperclientAddSpace client d = withTextUtf8 d $ \description -> do
  fmap (toEnum . fromIntegral) $ wrapHyperCall $ {#call hyperclient_add_space #} client description

-- enum hyperclient_returncode
-- hyperclient_rm_space(struct hyperclient* client, const char* space);
hyperclientRemoveSpace :: Hyperclient -> Text -> IO ReturnCode
hyperclientRemoveSpace client s = withTextUtf8 s $ \space -> do
  fmap (toEnum . fromIntegral) $ wrapHyperCall $ {#call hyperclient_rm_space #} client space
