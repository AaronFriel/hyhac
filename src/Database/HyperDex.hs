{-# LANGUAGE OverloadedStrings #-}
module Database.HyperDex
  ( hyhacVersion
  , module Database.HyperDex.Client
  , module Database.HyperDex.Admin
  , module Database.HyperDex.Internal.Core
  , module Database.HyperDex.Internal.Options
  )
	where

import Database.HyperDex.Client
import Database.HyperDex.Admin
import Database.HyperDex.Internal.Core (AsyncResult, Stream, readStream)
import Database.HyperDex.Internal.Options (defaultConnectInfo)

hyhacVersion :: String
hyhacVersion = "0.2.0.0"
