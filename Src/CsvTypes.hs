module CsvTypes where

import Control.Applicative
import qualified Data.Csv as Csv
import Data.Csv ((.:))
import Data.Maybe
import Data.Text

import Database.Persist.TH (derivePersistField)

data District = District
  { id   :: Int
  , name :: Text
  }

data Subject = Math | Reading | Writing | Science | SocialStudies deriving (Eq, Show, Read)
derivePersistField "Subject"

subjectFromString :: Text -> Maybe Subject
subjectFromString "Math" = Just Math
subjectFromString "Reading" = Just Reading
subjectFromString "Writing" = Just Writing
subjectFromString "Science" = Just Science
subjectFromString "SocialStudies" = Just SocialStudies
subjectFromString _ = Nothing

instance Csv.FromNamedRecord (Maybe Subject) where
  parseNamedRecord r = subjectFromString <$> r .: "Subject Name"

data Subgroup = All | MultiRace | White | Disabled
              | Migrant | Asian | PacificIslander
              | Hispanic | Male | Female
              | EconomicallyDisadvantaged | ESL | Native | Black deriving (Eq, Show, Read)
derivePersistField "Subgroup"

subgroupFromString :: Text -> Maybe Subgroup
subgroupFromString "Two or More Races" = Just MultiRace
subgroupFromString "White, n:ot of Hispanic origin" = Just White
subgroupFromString "Students with Disabilities" = Just Disabled
subgroupFromString "Migrant" = Just Migrant
subgroupFromString "Asian" = Just Asian
subgroupFromString "Native Hawaiian or Other Pacific Islander" = Just PacificIslander
subgroupFromString "Male" = Just Male
subgroupFromString "Hispanic" = Just Hispanic
subgroupFromString "Female" = Just Female
subgroupFromString "All Students" = Just All
subgroupFromString "Economically Disadvantaged" = Just EconomicallyDisadvantaged
subgroupFromString "English Language Learners" = Just ESL
subgroupFromString "American Indian or Alaskan Native" = Just Native
subgroupFromString "Black, not of Hispanic origin" = Just Black
subgroupFromString _ = Nothing

instance Csv.FromNamedRecord (Maybe Subgroup) where
  parseNamedRecord r = subgroupFromString <$> r .: "Subgroup"

data MEAPScore = MEAPScore
  { meapDistrict :: District
  , grade :: Int
  , subject :: Maybe Subject
  , subgroup :: Maybe Subgroup
  , numTested :: Int
  , level1Proficient :: Double
  , level2Proficient :: Double
  , level3Proficient :: Double
  , level4Proficient :: Double
  , totalProficient :: Double
  }

instance Csv.FromNamedRecord (Maybe MEAPScore) where
  parseNamedRecord r = optional $
    MEAPScore <$>
      (District <$> r .: "DistrictCode"
        <*> r .: "DistrictName")
      <*> r .: "Grade"
      <*> Csv.parseNamedRecord r
      <*> Csv.parseNamedRecord r
      <*> r .: "Number Tested"
      <*> r .: "Level 1 Proficient"
      <*> r .: "Level 2 Proficient"
      <*> r .: "Level 3 Proficient"
      <*> r .: "Level 4 Proficient"
      <*> r .: "Percent Proficient"

isValidMeap :: MEAPScore -> Bool
isValidMeap m = (isJust $ subject m) && (isJust $ subgroup m)

data SchoolStaff = SchoolStaff
  { staffDistrict :: District
  , teachers :: Double
  , librarians :: Double
  , librarySupport :: Double
  }

instance Csv.FromNamedRecord (Maybe SchoolStaff) where
  parseNamedRecord r = optional $
    SchoolStaff <$>
      (District <$> r .: "DCODE"
        <*> r .: "DNAME")
      <*> (r.: "TEACHER" <|> pure 0)
      <*> (r .: "LIB_SPEC" <|> pure 0)
      <*> (r .: "LIB_SUPP" <|> pure 0)
