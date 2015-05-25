module Main where

import CsvTypes as Mcsv
import SqlTypes as Msql

import Control.Monad.IO.Class
import Control.Monad.Reader

import qualified Data.ByteString.Lazy as BL
import qualified Data.Csv as Csv
import Data.Maybe
import Data.Monoid
import qualified Data.Vector as V

import Database.Esqueleto
import Database.Persist.Sql (runMigrationSilent)
import Database.Persist.Sqlite (runSqlite)

import qualified Options.Applicative as Opt

data ParseType = MEAP | Staff deriving (Read, Show)
data ParsedOptions = ParsedOptions { parseType :: ParseType, fileName :: String }

getFileName :: Opt.Parser ParsedOptions
getFileName = ParsedOptions <$>
  Opt.option Opt.auto (Opt.short 't' <> Opt.long "type" <> Opt.metavar "type" <> Opt.value MEAP)
  <*> Opt.argument Opt.str (Opt.metavar "file")

getDistrict :: Mcsv.District -> Msql.District
getDistrict m = Msql.District (Mcsv.id m) (Mcsv.name m)

getDistrictFromMEAP :: Mcsv.MEAPScore -> Msql.District
getDistrictFromMEAP m = getDistrict $ Mcsv.meapDistrict m

getDistrictFromStaff :: Mcsv.SchoolStaff -> Msql.District
getDistrictFromStaff m = getDistrict $ Mcsv.staffDistrict m

runInsert v insertFunc = runSqlite "meap.db" $ do
  runMigrationSilent migrateTables
  V.forM_ v insertFunc

getScore :: Mcsv.MEAPScore -> Msql.DistrictId -> Msql.MEAPScore
getScore m d = Msql.MEAPScore d (Mcsv.grade m) (fromJust $ Mcsv.subject m)
  (fromJust $ Mcsv.subgroup m) (Mcsv.numTested m) (Mcsv.level1Proficient m)
  (Mcsv.level2Proficient m) (Mcsv.level3Proficient m) (Mcsv.level4Proficient m)
  (Mcsv.totalProficient m)

insertScore :: (MonadIO m, backend ~ PersistEntityBackend Msql.MEAPScore) => Maybe Mcsv.MEAPScore -> ReaderT backend m ()
insertScore (Just m)
  | Mcsv.isValidMeap m = do
    let sqlDistrict = getDistrictFromMEAP m
    -- check if district is already in database
    dids <- liftM (take 1) $ select $ from $ \d -> do
      where_ (d ^. Msql.DistrictSid ==. val (Msql.districtSid sqlDistrict))
      return d
    case dids of
      -- use found district if it exists
      [foundDistrict] -> insert_ $ getScore m $ entityKey foundDistrict
      -- insert district if not
      [] -> do
        districtKey <- insert sqlDistrict
        insert_ $ getScore m districtKey
  | otherwise = return ()
insertScore Nothing = return ()

getStaff :: Mcsv.SchoolStaff -> Msql.DistrictId -> Msql.SchoolStaff
getStaff m d = Msql.SchoolStaff d (Mcsv.teachers m) (Mcsv.librarians m)
  (Mcsv.librarySupport m)

insertSingleStaff (Just m) = do
  let sqlDistrict = getDistrictFromStaff m
  -- check if district is already in database
  dids <- liftM (take 1) $ select $ from $ \d -> do
    where_ (d ^. Msql.DistrictSid ==. val (Msql.districtSid sqlDistrict))
    return d
  case dids of
    -- use found district if it exists
    [foundDistrict] -> insert_ $ getStaff m $ entityKey foundDistrict
    -- insert district if not
    [] -> do
      districtKey <- insert sqlDistrict
      insert_ $ getStaff m districtKey
insertSingleStaff Nothing = return ()

main :: IO ()
main = do
  opts <- Opt.execParser rawopts
  csvData <- BL.readFile $ fileName opts
  case parseType opts of
    MEAP -> case Csv.decodeByName csvData of
      Left err -> putStrLn err
      Right (_, v :: (V.Vector (Maybe Mcsv.MEAPScore))) -> runInsert v insertScore
    Staff -> case Csv.decodeByName csvData of
      Left err -> putStrLn err
      Right (_, v :: (V.Vector (Maybe Mcsv.SchoolStaff))) -> runInsert v insertSingleStaff
  where
    rawopts = Opt.info getFileName Opt.fullDesc
