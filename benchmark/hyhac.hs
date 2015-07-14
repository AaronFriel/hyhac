{-# LANGUAGE OverloadedStrings #-}
import Criterion.Main
import Database.HyperDex
-- import qualified Database.Cassandra.Basic as C
import System.Environment(getEnv)
import Control.Monad --(forM_,forM,void,when,join,forever)
import Control.Applicative
import Control.DeepSeq

import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BSL
import qualified Data.Text as Text
import qualified Data.Text.IO as Text
import Data.Either(lefts)

import System.Process
-- import qualified Database.SQLite3 as SQL
import qualified Control.Exception as E

instance NFData Attribute where
  rnf (Attribute a b c) = a `seq` b `seq` c `seq` ()

main :: IO ()
main = do
  reps <- read  <$> getEnv "REPS"
  -- threaded <- (=="yes")  <$> getEnv "THREADED"

  _ <- system "rm -f dummysql"
  _ <- system "rm -f dummyfile"

  ws <- BS.lines <$> BS.readFile "/usr/share/dict/words"
  wsl <- BSL.lines <$> BSL.readFile "/usr/share/dict/words"
  wst <- Text.lines <$> Text.readFile "/usr/share/dict/words"

  let
      lastname = BS.take 11000 $!! BS.unlines ws
      lastnamel = BSL.take 11000 $!! BSL.unlines wsl
      lastnamet = Text.take 10000 $!! Text.unlines wst

      testrun = take reps $!! ws
      testrunl = take reps $!! wsl
      testrunt = take reps $!! wst

  return $ rnf lastname
--  return $ rnf testrun
  return $ rnf lastnamet
  --return $ rnf testrunt
  return $ rnf lastnamel
--  return $ rnf testrunl

  -- pool <- C.createCassandraPool C.defServers 3 300 5 "testkeyspace"

  admin <- adminConnect defaultConnectInfo

  E.handle ignore $ void $ rmSpace "phonebook" admin
  _ <- addSpace
       (BS.unlines
         [ "space phonebook"
         , "key username"
         , "attributes content"
         -- , "subspace first, last"
         , "create 32 partitions"
         , "tolerate 0 failures"
         ])
       admin

  client <- clientConnect defaultConnectInfo

  -- db <- SQL.open "dummysql"
  -- SQL.execPrint db "PRAGMA journal_mode=MEMORY; PRAGMA synchronous = OFF"
  --
  -- SQL.exec db "create table phonebook (username txt, content text);"
  -- stmt <- SQL.prepare db "insert into phonebook (username, content) values (?,?);"

  --file <- openFile "/dev/null" WriteMode

  let -- finish :: ((BS.ByteString, BSL.ByteString, Text.Text) -> IO a) -> ([a] -> IO ()) -> IO ()
      finish work ender = mapM work (zip3 testrun testrunl testrunt) >>= ender

  putStrLn "starting"
  defaultMain [
               -- bench "sqlite" $ do
               --    SQL.execPrint db "begin transaction;"
               --    finish ( \(_,_,x) -> do
               --             SQL.bind stmt [SQL.SQLText x, SQL.SQLText lastnamet]
               --             SQL.step stmt
               --             SQL.reset stmt)
               --      ( \_ -> SQL.execPrint db "end transaction;"  ),
               -- bench "file" $ do
               --   file <- openFile "./dummyfile" WriteMode
               --   finish (\(x,_,_) ->
               --            BS.hPutStrLn file $ BS.unlines [x, lastname])
               --     (\_ -> hClose file),
               -- bench "cassandra" $ finish (\(_,x,_) -> return $
               --                                C.insert  "phonebook" x C.ANY
               --                                  [ C.col "last" lastnamel
               --                                  ])
               --                            (\insertions ->  void $ C.runCas pool $ sequence insertions),
               bench "hyperdex" . whnfIO $ finish (\(x,_,_) ->
                                           put "phonebook" x [mkAttribute "content"  lastname] client)
                                         (\actions -> do
                                             failures <- lefts <$> sequence actions
                                             when (failures /= []) $
                                               error ("failure in hyperdex: " ++ show failures)
                                             return ())
              ]


  -- void $ system "echo 'use testkeyspace; drop table phonebook;' | cqlsh"

  _ <- system "rm -f dummysql"
  _ <- system "rm -f dummyfile"

  return ()

ignore :: E.SomeException -> IO ()
ignore _ = return ()
