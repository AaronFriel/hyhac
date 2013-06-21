{-# LANGUAGE OverloadedStrings #-}

module Test.HyperDex.Space where

import Data.Text
import Data.Monoid ((<>))
import Test.QuickCheck.Arbitrary

import Data.Int (Int64)
import Data.ByteString (ByteString)
import Data.Set (Set)
import Data.Map (Map)

import Database.HyperDex
import Database.HyperDex.Utf8

import Test.HyperDex.Util ()

defaultSpace :: Text
defaultSpace = "profiles"

defaultSpaceDesc :: Text
defaultSpaceDesc = makeSpaceDesc defaultSpace

makeSpaceDesc :: Text -> Text
makeSpaceDesc name =
  "space "<>name<>"                         \n\
  \key username                             \n\
  \attributes                               \n\
  \   string first,                         \n\
  \   string last,                          \n\
  \   float score,                          \n\
  \   int profile_views,                    \n\
  \   list(string) pending_requests,        \n\
  \   list(float) rankings,                 \n\
  \   list(int) todolist,                   \n\
  \   set(string) hobbies,                  \n\
  \   set(float) imonafloat,                \n\
  \   set(int) friendids,                   \n\
  \   map(string, string) unread_messages,  \n\
  \   map(string, int) upvotes,             \n\
  \   map(string, float) friendranks,       \n\
  \   map(int, string) posts,               \n\
  \   map(int, int) friendremapping,        \n\
  \   map(int, float) intfloatmap,          \n\
  \   map(float, string) still_looking,     \n\
  \   map(float, int) for_a_reason,         \n\
  \   map(float, float) for_float_keyed_map \n\
  \create 10 partitions                     \n\
  \tolerate 2 failures"

newtype DefaultSpaceAttributes = DefaultSpaceAttributes { unDefaultSpace :: [Attribute] }
  deriving (Show, Eq)

instance Arbitrary DefaultSpaceAttributes where
  arbitrary = do
    (first', last', score', profile_views',
      (pending_requests', rankings', todolist', hobbies',
        (imonafloat', friendids', unread_messages', upvotes',
          (friendranks', posts', friendremapping', intfloatmap',
            (still_looking', for_a_reason', for_float_keyed_map')))))
      <- arbitrary
    return $ DefaultSpaceAttributes
                [ mkAttributeUtf8 "first"               (first'               :: ByteString               )
                , mkAttributeUtf8 "last"                (last'                :: ByteString               )
                , mkAttributeUtf8 "score"               (score'               :: Double                   )
                , mkAttributeUtf8 "profile_views"       (profile_views'       :: Int64                    )
                , mkAttributeUtf8 "pending_requests"    (pending_requests'    :: [ByteString]             )
                , mkAttributeUtf8 "rankings"            (rankings'            :: [Double]                 )
                , mkAttributeUtf8 "todolist"            (todolist'            :: [Int64]                  )
                , mkAttributeUtf8 "hobbies"             (hobbies'             :: Set ByteString           )
                , mkAttributeUtf8 "imonafloat"          (imonafloat'          :: Set Double               )
                , mkAttributeUtf8 "friendids"           (friendids'           :: Set Int64                )
                , mkAttributeUtf8 "unread_messages"     (unread_messages'     :: Map ByteString ByteString)
                , mkAttributeUtf8 "upvotes"             (upvotes'             :: Map ByteString Int64     )
                , mkAttributeUtf8 "friendranks"         (friendranks'         :: Map ByteString Double    )
                , mkAttributeUtf8 "posts"               (posts'               :: Map Int64      ByteString)
                , mkAttributeUtf8 "friendremapping"     (friendremapping'     :: Map Int64      Int64     )
                , mkAttributeUtf8 "intfloatmap"         (intfloatmap'         :: Map Int64      Double    )
                , mkAttributeUtf8 "still_looking"       (still_looking'       :: Map Double     ByteString)
                , mkAttributeUtf8 "for_a_reason"        (for_a_reason'        :: Map Double     Int64     )
                , mkAttributeUtf8 "for_float_keyed_map" (for_float_keyed_map' :: Map Double     Double    )
                ]

pickAttributeName :: HyperSerialize a => a -> ByteString
pickAttributeName value =
  case datatype value of 
    HyperdatatypeString           -> "first"
    HyperdatatypeInt64            -> "profile_views"
    HyperdatatypeFloat            -> "score"
    HyperdatatypeListString       -> "pending_requests"
    HyperdatatypeListInt64        -> "todolist"
    HyperdatatypeListFloat        -> "rankings"
    HyperdatatypeSetString        -> "hobbies"
    HyperdatatypeSetInt64         -> "friendids"
    HyperdatatypeSetFloat         -> "imonafloat"
    HyperdatatypeMapStringString  -> "unread_messages"
    HyperdatatypeMapStringInt64   -> "upvotes"
    HyperdatatypeMapStringFloat   -> "friendranks"
    HyperdatatypeMapInt64String   -> "posts"
    HyperdatatypeMapInt64Int64    -> "friendremapping"
    HyperdatatypeMapInt64Float    -> "intfloatmap"
    HyperdatatypeMapFloatString   -> "still_looking"
    HyperdatatypeMapFloatInt64    -> "for_a_reason"
    HyperdatatypeMapFloatFloat    -> "for_float_keyed_map"
    _                             -> error "Invalid data type"

keyAttributeName :: ByteString
keyAttributeName = "username"
