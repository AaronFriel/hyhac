module Database.HyperDex.Internal.Space 
  ( addSpace, removeSpace
  )
  where

{#import Database.HyperDex.Internal.Client #}
{#import Database.HyperDex.Internal.ReturnCode #}
import Database.HyperDex.Internal.Util

#import "hyperclient.h"

addSpace :: Client -> ByteString -> IO HyperclientReturnCode
addSpace c desc  = withClientImmediate c $ \hc -> do
  hyperclientAddSpace hc desc

removeSpace :: Client -> ByteString -> IO HyperclientReturnCode
removeSpace c name = withClientImmediate c $ \hc -> do
  hyperclientRemoveSpace hc name

-- enum hyperclient_returncode
-- hyperclient_add_space(struct hyperclient* client, const char* description);
hyperclientAddSpace :: Hyperclient -> ByteString -> IO HyperclientReturnCode
hyperclientAddSpace client d = withCBString d $ \description -> do
  fmap (toEnum . fromIntegral) $ {#call hyperclient_add_space #} client description

-- enum hyperclient_returncode
-- hyperclient_rm_space(struct hyperclient* client, const char* space);
hyperclientRemoveSpace :: Hyperclient -> ByteString -> IO HyperclientReturnCode
hyperclientRemoveSpace client s = withCBString s $ \space -> do
  fmap (toEnum . fromIntegral) $ {#call hyperclient_rm_space #} client space
