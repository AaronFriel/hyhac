{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE FlexibleInstances, ViewPatterns #-}

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
-- A helper function to create attributes is exported.
--
-----------------------------------------------------------------------------

module Database.HyperDex.Utf8
  ( mkAttributeUtf8
  , mkAttributeCheckUtf8
  , mkMapAttributeUtf8
  , mkMapAttributesFromMapUtf8
  )
  where

import Database.HyperDex.Internal.Data.Attribute
import Database.HyperDex.Internal.Data.AttributeCheck
import Database.HyperDex.Internal.Data.MapAttribute
import Database.HyperDex.Internal.Serialize
import Database.HyperDex.Internal.Data.Hyperdex
import Data.Serialize

import Data.Text (Text)
import qualified Data.Text as Text
import Data.Text.Encoding

import Data.Map (Map)

instance HyperSerialize [Char] where
  getH = remaining >>= getByteString >>= return . Text.unpack . decodeUtf8
  putH = putByteString . encodeUtf8 . Text.pack
  datatype = const HyperdatatypeString

instance HyperSerialize Text where
  getH = remaining >>= getByteString >>= return . decodeUtf8
  putH = putByteString . encodeUtf8
  datatype = const HyperdatatypeString

-- | Create an attribute using a name serialized as a UTF8 bytestring.
mkAttributeUtf8 :: HyperSerialize a => Text -> a -> Attribute
mkAttributeUtf8 (encodeUtf8 -> name) value = mkAttribute name value

-- | Create an attribute using a name serialized as a UTF8 bytestring.
mkAttributeCheckUtf8 :: HyperSerialize a => Text -> a -> Hyperpredicate -> AttributeCheck
mkAttributeCheckUtf8 (encodeUtf8 -> name) = mkAttributeCheck name

-- | Create an attribute using a name serialized as a UTF8 bytestring.
mkMapAttributeUtf8 :: (HyperSerialize k, HyperSerialize v) => Text -> k -> v -> MapAttribute
mkMapAttributeUtf8 (encodeUtf8 -> name) = mkMapAttribute name

-- | Create an attribute using a name serialized as a UTF8 bytestring.
mkMapAttributesFromMapUtf8 :: (HyperSerialize k, HyperSerialize v) => Text -> Map k v -> [MapAttribute]
mkMapAttributesFromMapUtf8 = mkMapAttributesFromMap . encodeUtf8
