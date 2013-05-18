{-# LANGUAGE FlexibleInstances #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Data.HyperDex.Client
-- Copyright   :  (c) Aaron Friel 2013
-- License     :  BSD-style
-- Maintainer  :  mayreply@aaronfriel.com
-- Stability   :  maybe
-- Portability :  portable
--
-- UTF8 String and Text support for HyperDex.
--
-- Only instances are exported.
--
-----------------------------------------------------------------------------

module Database.HyperDex.Utf8
  ( )
  where

import Database.HyperDex.Internal.Attribute
import Database.HyperDex.Internal.Hyperdata
import Database.HyperDex.Internal.Hyperdex
import Data.Serialize

import Data.Text (Text)
import qualified Data.Text as Text
import Data.Text.Encoding

instance HyperSerialize [Char] where
  getH = remaining >>= getByteString >>= return . Text.unpack . decodeUtf8
  putH = putByteString . encodeUtf8 . Text.pack
  datatype = const HyperdatatypeString

instance HyperSerialize Text where
  getH = remaining >>= getByteString >>= return . decodeUtf8
  putH = putByteString . encodeUtf8
  datatype = const HyperdatatypeString

instance AttributeName String where
  nameToBS = encodeUtf8 . Text.pack

instance AttributeName Text where
  nameToBS = encodeUtf8
