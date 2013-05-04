module Database.HyperDex.Internal.Hyperdex where

-- import Foreign.C.Types
-- import Foreign.C.String
-- import Foreign.Marshal.Utils
-- import Foreign.Marshal.Alloc
-- import Foreign.Ptr
-- import Foreign.Storable
-- import Data.Int

-- import Control.Applicative ((<$>))

#import <stdbool.h>
#import "hyperdex.h"

#c
int container_type(int X)
{
  return (CONTAINER_TYPE(X));
}

int container_elem(int X)
{
  return (CONTAINER_ELEM(X));
}

int container_val(int X)
{
  return (CONTAINER_VAL(X));
}

int container_key(int X)
{
  return (CONTAINER_KEY(X));
}

int is_primitive(int X)
{
  return (IS_PRIMITIVE(X));
}

int create_container(int C, int E)
{
  return (CREATE_CONTAINER(C, E));
}

int create_container2(int C, int K, int V)
{
  return (CREATE_CONTAINER2(C, K, V));
}
#endc

{#enum hyperdatatype as Hyperdatatype {underscoreToCase} deriving (Eq, Show) #}

{#enum hyperpredicate as Hyperpredicate {underscoreToCase} deriving (Eq, Show) #}
