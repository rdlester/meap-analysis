module SqlTypes where

import qualified CsvTypes as Mcsv

import Data.Text
import Database.Persist.TH (mkPersist, mkMigrate, persistLowerCase,
                           share, sqlSettings)

share [mkPersist sqlSettings, mkMigrate "migrateTables"] [persistLowerCase|
District
  sid               Int
  name              Text
  Primary sid
MEAPScore
  meapDistrict      DistrictId
  year              Text
  grade             Int
  subject           Mcsv.Subject
  subgroup          Mcsv.Subgroup
  numTested         Int
  level1Proficient  Double
  level2Proficient  Double
  level3Proficient  Double
  level4Proficient  Double
  totalProficient   Double
  avgScore          Double
  stdDev            Double
  deriving Show
SchoolStaff
  staffDistrict     DistrictId
  teachers          Double
  librarians        Double
  librarySupport    Double
|]
