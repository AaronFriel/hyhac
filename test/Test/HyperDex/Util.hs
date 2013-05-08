{-# LANGUAGE FlexibleInstances #-}

module Test.HyperDex.Util where

import Test.QuickCheck
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (pack, unpack)

char :: Gen Char
char = choose ('\0', '\255')

nonNulChar :: Gen Char
nonNulChar = choose ('\1', '\255')

lowerAlphabeticChar :: Gen Char
lowerAlphabeticChar = choose ('a','z')

data ArbitraryEmpty = AllowEmpty | DisallowEmpty

byteStringFromChar :: ArbitraryEmpty -> Gen Char -> Gen ByteString
byteStringFromChar AllowEmpty char = fmap pack . listOf $ char
byteStringFromChar DisallowEmpty char = fmap pack . listOf1 $ char

arbitraryByteStringIdentifier :: Gen ByteString
arbitraryByteStringIdentifier = byteStringFromChar DisallowEmpty lowerAlphabeticChar

arbitraryNonNulByteString :: ArbitraryEmpty -> Gen ByteString
arbitraryNonNulByteString p = byteStringFromChar p nonNulChar

arbitraryByteString :: ArbitraryEmpty -> Gen ByteString
arbitraryByteString p = byteStringFromChar p char

instance Arbitrary ByteString where
  arbitrary = arbitraryByteString AllowEmpty
  -- | Remove substrings of decreasing size, a la quicksort:
  shrink = map pack . shrink . unpack 
