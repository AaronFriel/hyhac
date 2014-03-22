module Chaos.Concurrent.STM.TVar
     ( TVar
	   , newTVar
	   , writeTVar
	   , readTVar
	   , modifyTVar
     )
 where

import Control.Concurrent.STM.TVar (TVar)
import qualified Control.Concurrent.STM.TVar as TVar
import Control.Exception

import Chaos.Doomsday

import GHC.Conc (unsafeIOToSTM)

newTVar = withSTMChaos . TVar.newTVar 
writeTVar v i = withSTMChaos $ TVar.writeTVar v i
readTVar = withSTMChaos . TVar.readTVar
modifyTVar v f = withSTMChaos $ TVar.modifyTVar v f

withSTMChaos = withDoomsdaySTM [mkE BlockedIndefinitelyOnSTM]
