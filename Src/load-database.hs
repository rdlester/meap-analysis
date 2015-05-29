module Main where

import CsvTypes as Mcsv
import SqlTypes as Msql

import Control.Applicative
import Control.Monad.IO.Class
import Control.Monad.Reader
import Control.Monad.Trans.Control

import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.Csv.Streaming as CsvStream
import Data.Monoid
import qualified Data.Set as Set
import qualified Data.Vector as V

import Database.Esqueleto
import Database.Persist.Sqlite (runSqlite)

import qualified Options.Applicative as Opt

-- options parsing

data ParseType = MEAP | Staff deriving (Read, Show)
data ParsedOptions = ParsedOptions { parseType :: ParseType, fileName :: String }

getFileName :: Opt.Parser ParsedOptions
getFileName = ParsedOptions <$>
  Opt.option Opt.auto (Opt.short 't' <> Opt.long "type" <> Opt.metavar "type" <> Opt.value MEAP)
  <*> Opt.argument Opt.str (Opt.metavar "file")

-- csv to sql conversions

csvDistrictToSqlDistrict :: Mcsv.District -> Msql.District
csvDistrictToSqlDistrict m = Msql.District (Mcsv.id m) (Mcsv.name m)

csvScoreToSqlDistrict :: Mcsv.MEAPScore -> Msql.District
csvScoreToSqlDistrict m = csvDistrictToSqlDistrict $ Mcsv.meapDistrict m

csvStaffToSqlDistrict :: Mcsv.SchoolStaff -> Msql.District
csvStaffToSqlDistrict m = csvDistrictToSqlDistrict $ Mcsv.staffDistrict m

csvScoreToSqlScore :: Mcsv.MEAPScore -> Msql.DistrictId -> Msql.MEAPScore
csvScoreToSqlScore
  (Mcsv.MEAPScore _ y g subj subgrp num l1 l2 l3 l4 total avg err) d =
    Msql.MEAPScore d y g subj subgrp num l1 l2 l3 l4 total avg err

csvStaffToSqlStaff :: Mcsv.SchoolStaff -> Msql.DistrictId -> Msql.SchoolStaff
csvStaffToSqlStaff (Mcsv.SchoolStaff _ t l ls) d = Msql.SchoolStaff d t l ls

-- database insertion

insertCsvToSql ::
  (PersistEntity val, MonadIO m, PersistStore (PersistEntityBackend val),
    PersistEntityBackend val ~ SqlBackend) =>
  (t -> Msql.District) -> (t -> Key Msql.District -> val) -> Maybe t
    -> ReaderT SqlBackend m ()
insertCsvToSql csvToSqlDistrict csvToSql (Just m) = do
  let sqlDistrict = csvToSqlDistrict m
  dids <- liftM (take 1) $ select $ from $ \d -> do
    where_ (d ^. Msql.DistrictSid ==. val (Msql.districtSid sqlDistrict))
    return d
  case dids of
    -- use found district if it exists
    (foundDistrict : _) -> insert_ $ csvToSql m $ entityKey foundDistrict
    -- insert district if not
    [] -> do
      districtKey <- insert sqlDistrict
      insert_ $ csvToSql m districtKey
insertCsvToSql _ _ _ = return ()

runInsertRecords ::
  (PersistEntity val, MonadIO m, PersistStore (PersistEntityBackend val),
    MonadBaseControl IO m, PersistEntityBackend val ~ SqlBackend) =>
  (t -> Msql.District) -> (t -> Key Msql.District -> val)
    -> CsvStream.Records t ->  m ()
runInsertRecords csvDistrictToSql csvValToSql r = runSqlite "meap.db" $ do
  _ <- runMigrationSilent migrateTables
  recurseOnRecords r Set.empty
  where
    recurseOnRecords (CsvStream.Cons (Left err) rest) errSet = do
      printErr err errSet
      recurseOnRecords rest $ Set.insert err errSet
    recurseOnRecords (CsvStream.Cons (Right v) rest) errSet = do
      insertCsvToSql csvDistrictToSql csvValToSql $ Just v
      recurseOnRecords rest errSet
    recurseOnRecords (CsvStream.Nil (Just err) remaining) _ = do
      liftIO $ putStrLn err
      liftIO $ putStrLn $ BLC.unpack remaining
    recurseOnRecords (CsvStream.Nil Nothing remaining) _ =
      liftIO $ putStrLn $ BLC.unpack remaining
    printErr "empty" _ = return ()
    printErr err errSet
      | Set.notMember err errSet = liftIO $ putStrLn err
      | otherwise = return ()

runInsert ::
  (PersistEntity val, MonadIO m, PersistStore (PersistEntityBackend val),
    MonadBaseControl IO m, PersistEntityBackend val ~ SqlBackend) =>
  (t -> Msql.District) -> (t -> Key Msql.District -> val)
    -> V.Vector (Maybe t) ->  m ()
runInsert csvDistrictToSql csvValToSql v = runSqlite "meap.db" $ do
  _ <- runMigrationSilent migrateTables
  V.forM_ v $ insertCsvToSql csvDistrictToSql csvValToSql

-- main

main :: IO ()
main = do
  opts <- Opt.execParser $ Opt.info getFileName Opt.fullDesc
  csvData <- BL.readFile $ fileName opts
  case parseType opts of
    MEAP -> case CsvStream.decodeByName csvData of
      Left err -> putStrLn err
      Right (_, r :: (CsvStream.Records Mcsv.MEAPScore)) ->
        runInsertRecords csvScoreToSqlDistrict csvScoreToSqlScore r
    Staff -> case CsvStream.decodeByName csvData of
      Left err -> putStrLn err
      Right (_, r :: (CsvStream.Records Mcsv.SchoolStaff)) ->
        runInsertRecords csvStaffToSqlDistrict csvStaffToSqlStaff r
