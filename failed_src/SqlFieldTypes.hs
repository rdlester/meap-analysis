module SqlFieldTypes where

import Database.Persist.TH (derivePersistField)

data Subject = Math | Reading | Writing | Science | SocialStudies deriving (Eq, Show, Read)
derivePersistField "Subject"

data Subgroup = All | MultiRace | White | Disabled
              | Migrant | Asian | PacificIslander
              | Hispanic | Male | Female
              | EconomicallyDisadvantaged | ESL | Native | Black deriving (Eq, Show, Read)
derivePersistField "Subgroup"

newtype Proficiency = Proficiency Double deriving (Eq, Show, Read)
derivePersistField "Proficiency"
