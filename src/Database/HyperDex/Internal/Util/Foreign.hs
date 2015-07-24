{-# LANGUAGE FlexibleContexts #-}
-- |
-- Module       : Database.HyperDex.Internal.Util.Foreign
-- Copyright    : (c) Aaron Friel 2014
-- License      : BSD-style
-- Maintainer   : mayreply@aaronfriel.com
-- Stability    : unstable
-- Portability  : portable
--

module Database.HyperDex.Internal.Util.Foreign
 ( module Foreign
 , module Foreign.C.Types
 )
 where

-- See Note [Foreign imports]
import Foreign 
 ( Storable (sizeOf, alignment, peek, peekByteOff, poke, pokeByteOff)
 , Ptr
 , advancePtr
 , peekArray
 )
import Foreign.C.Types

{- Note [Foreign imports]
~~~~~~~~~~~~~~~~~~~~~~

A minimal set of foreign functions are exported here. A memory safe,
bounds-checked version may be substituted in as a replacement for these.

These re-exports do not inlude any allocation, as all allocation
should be done through the Resource module. See Note [Resource handling]
in Resource.hs.
-}