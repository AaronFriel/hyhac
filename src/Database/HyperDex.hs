module Database.HyperDex
  ( hyhacVersion
  , module Database.HyperDex.Client
  , module Database.HyperDex.Admin
  )
	where

import Database.HyperDex.Client
import Database.HyperDex.Admin

hyhacVersion :: String
hyhacVersion = "0.2.0.0"
