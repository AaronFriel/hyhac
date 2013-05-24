{-# LANGUAGE OverloadedStrings #-}

module Test.HyperDex.Space where

import Data.Text
import Data.Monoid ((<>))

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