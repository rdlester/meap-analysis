module Main where

import CsvTypes as Mcsv
import SqlTypes as Msql

import Control.Monad.IO.Class
import Control.Monad.Reader

import qualified Data.ByteString.Lazy as BL
import qualified Data.Csv as Csv
import Control.Monad.Trans.Control
import Data.Monoid
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
  (Mcsv.MEAPScore _ g subj subgrp num l1 l2 l3 l4 total) d =
    Msql.MEAPScore d g subj subgrp num l1 l2 l3 l4 total

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

runInsert ::
  (PersistEntity val, MonadIO m, PersistStore (PersistEntityBackend val),
    MonadBaseControl IO m, PersistEntityBackend val ~ SqlBackend) =>
  V.Vector (Maybe t) -> (t -> Msql.District)
    -> (t -> Key Msql.District -> val) -> m ()
runInsert v csvDistrictToSql csvValToSql = runSqlite "meap.db" $ do
  _ <- runMigrationSilent migrateTables
  V.forM_ v $ insertCsvToSql csvDistrictToSql csvValToSql

-- main

main :: IO ()
main = do
  opts <- Opt.execParser rawopts
  csvData <- BL.readFile $ fileName opts
  case parseType opts of
    MEAP -> case Csv.decodeByName csvData of
      Left err -> putStrLn err
      Right (_, v :: (V.Vector (Maybe Mcsv.MEAPScore))) -> runInsert v csvScoreToSqlDistrict csvScoreToSqlScore
    Staff -> case Csv.decodeByName csvData of
      Left err -> putStrLn err
      Right (_, v :: (V.Vector (Maybe Mcsv.SchoolStaff))) -> runInsert v csvStaffToSqlDistrict csvStaffToSqlStaff
  where
    rawopts = Opt.info getFileName Opt.fullDesc
