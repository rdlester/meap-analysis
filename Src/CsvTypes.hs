module CsvTypes where

import Control.Applicative
import qualified Data.Csv as Csv
import Data.Csv ((.:))
import Data.Either
import Data.Text

import Database.Persist.TH (derivePersistField)

data District = District
  { id   :: Int
  , name :: Text
  }

data Subject = Math | Reading | Writing | Science | SocialStudies deriving (Eq, Show, Read)
derivePersistField "Subject"

instance Csv.FromField Subject where
  parseField "Math" = pure Math
  parseField "Reading" = pure Reading
  parseField "Writing" = pure Writing
  parseField "Science" = pure Science
  parseField "SocialStudies" = pure SocialStudies
  parseField _ = Control.Applicative.empty

data Subgroup = All | MultiRace | White | Disabled
              | Migrant | Asian | PacificIslander
              | Hispanic | Male | Female
              | EconomicallyDisadvantaged | ESL | Native | Black deriving (Eq, Show, Read)
derivePersistField "Subgroup"

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

newtype Proficiency = Proficiency { proficiency :: Double }
  deriving (Eq, Show, Read)

instance Csv.FromField Proficiency where
  parseField "< 5%" = pure $ Proficiency 5
  parseField "> 95%" = pure $ Proficiency 95
  parseField f = Proficiency <$> Csv.parseField f

data MEAPScore = MEAPScore
  { meapDistrict :: District
  , year :: Text
  , grade :: Int
  , subject :: Subject
  , subgroup :: Subgroup
  , numTested :: Int
  , level1Proficient :: Double
  , level2Proficient :: Double
  , level3Proficient :: Double
  , level4Proficient :: Double
  , totalProficient :: Double
  , avgScore :: Double
  , stdDev :: Double
  }

instance Csv.FromNamedRecord MEAPScore where
  parseNamedRecord r =
    -- Only parse rows covering entire districts.
    if isLeft $ Csv.runParser ((r .: "BuildingCode") :: Csv.Parser Int)
      then
        MEAPScore <$>
          (District <$> r .: "DistrictCode"
            <*> r .: "DistrictName")
          <*> r .: "SchoolYear"
          <*> r .: "Grade"
          <*> r .: "Subject Name"
          <*> r .: "Subgroup"
          <*> r .: "Number Tested"
          <*> (proficiency <$> r .: "Level 1 Proficient")
          <*> (proficiency <$> r .: "Level 2 Proficient")
          <*> (proficiency <$> r .: "Level 3 Proficient")
          <*> (proficiency <$> r .: "Level 4 Proficient")
          <*> (proficiency <$> r .: "Percent Proficient")
          <*> r .: "Average Scaled Score"
          <*> r .: "Standard Deviation"
      else
        Control.Applicative.empty

instance Csv.FromNamedRecord (Maybe MEAPScore) where
  parseNamedRecord r = optional $ Csv.parseNamedRecord r

data SchoolStaff = SchoolStaff
  { staffDistrict :: District
  , teachers :: Double
  , librarians :: Double
  , librarySupport :: Double
  }

instance Csv.FromNamedRecord SchoolStaff where
  parseNamedRecord r =
    SchoolStaff <$>
      (District <$> r .: "DCODE"
        <*> r .: "DNAME")
      <*> (r.: "TEACHER" <|> pure 0)
      <*> (r .: "LIB_SPEC" <|> pure 0)
      <*> (r .: "LIB_SUPP" <|> pure 0)

instance Csv.FromNamedRecord (Maybe SchoolStaff) where
  parseNamedRecord r = optional $ Csv.parseNamedRecord r
