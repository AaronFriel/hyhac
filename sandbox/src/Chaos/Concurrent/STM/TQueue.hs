module Chaos.Concurrent.STM.TQueue
     ( TQueue
	   , newTQueue
	   , writeTQueue
	   , readTQueue
	   , tryReadTQueue
     )
 where

import Control.Concurrent.STM.TQueue (TQueue)
import qualified Control.Concurrent.STM.TQueue as TQueue
import Control.Exception

import Chaos.Doomsday

import GHC.Conc (unsafeIOToSTM)

newTQueue = withSTMChaos $ TQueue.newTQueue
writeTQueue q i = withSTMChaos $ TQueue.writeTQueue q i
readTQueue = withSTMChaos . TQueue.readTQueue
tryReadTQueue = withSTMChaos . TQueue.tryReadTQueue

withSTMChaos = withDoomsdaySTM [mkE BlockedIndefinitelyOnSTM]
