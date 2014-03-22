module Chaos.IORef
     ( IORef
     , newIORef
     , readIORef
     , writeIORef
     )
 where

import Data.IORef (IORef)
import qualified Data.IORef as IORef
import Control.Exception

import Chaos.Doomsday

newIORef = withChaos . IORef.newIORef
readIORef = withChaos . IORef.readIORef
writeIORef r v = withChaos $ IORef.writeIORef r v

withChaos = withDoomsday [mkE $ ErrorCall "Chaos"]
