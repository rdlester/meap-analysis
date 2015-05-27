{-# OPTIONS_GHC -ddump-splices #-}
module Types where

import SqlFieldTypes

import Control.Applicative
import qualified Data.Csv as Csv
import Data.Csv ((.:))
import Data.Text
import Database.Persist
import Database.Persist.Class
import Database.Persist.Types
import Database.Persist.TH (mkPersist, mkMigrate, persistLowerCase,
                           share, sqlSettings, derivePersistField)

share [mkPersist sqlSettings, mkMigrate "migrateTables"] [persistLowerCase|
District
  sid               Int
  name              Text
  Primary sid
MEAPScore
  meapDistrict      DistrictId
  grade             Int
  subject           Subject
  subgroup          Subgroup
  numTested         Int
  level1Proficient  Proficiency
  level2Proficient  Proficiency
  level3Proficient  Proficiency
  level4Proficient  Proficiency
  totalProficient   Proficiency
  deriving Show
SchoolStaff
  staffDistrict     DistrictId
  teachers          Double
  librarians        Double
  librarySupport    Double
|]

instance Csv.FromField Subject where
  parseField "Math" = pure Math
  parseField "Reading" = pure Reading
  parseField "Writing" = pure Writing
  parseField "Science" = pure Science
  parseField "SocialStudies" = pure SocialStudies
  parseField _ = Control.Applicative.empty

instance Csv.FromField Subgroup where
  parseField "Two or More Races" = pure MultiRace
  parseField "White, n:ot of Hispanic origin" = pure White
  parseField "Students with Disabilities" = pure Disabled
  parseField "Migrant" = pure Migrant
  parseField "Asian" = pure Asian
  parseField "Native Hawaiian or Other Pacific Islander" = pure PacificIslander
  parseField "Male" = pure Male
  parseField "Hispanic" = pure Hispanic
  parseField "Female" = pure Female
  parseField "All Students" = pure All
  parseField "Economically Disadvantaged" = pure EconomicallyDisadvantaged
  parseField "English Language Learners" = pure ESL
  parseField "American Indian or Alaskan Native" = pure Native
  parseField "Black, not of Hispanic origin" = pure Black
  parseField _ = Control.Applicative.empty

instance Csv.FromField Proficiency where
  parseField "< 5%" = pure $ Proficiency 5
  parseField "> 95%" = pure $ Proficiency 95
  parseField f = Proficiency <$> Csv.parseField f

instance Csv.FromNamedRecord (Maybe (District, MEAPScore)) where
  parseNamedRecord r = optional $ (,) <$>
      (District <$> r .: "DistrictCode"
        <*> r .: "DistrictName")
    <*> (MEAPScore <$>
        r .: "DistrictCode"
      <*> r .: "Grade"
      <*> r .: "Subject Name"
      <*> r .: "Subgroup"
      <*> r .: "Number Tested"
      <*> r .: "Level 1 Proficient"
      <*> r .: "Level 2 Proficient"
      <*> r .: "Level 3 Proficient"
      <*> r .: "Level 4 Proficient"
      <*> r .: "Percent Proficient")
