#!/usr/bin/env runhaskell
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Applicative
import qualified Data.ByteString.Lazy as BL
import qualified Data.Csv as Csv
import qualified Data.Vector as V

import qualified Options.Applicative as Opt

data District = District
  { id   :: Int
  , name :: String
  }

data MEAPScore = MEAPScore
  { district :: District
  , grade :: Int
  , subject :: String
  , subgroup :: String
  , numTested :: Int
  , level1Proficient :: Float
  , level2Proficient :: Float
  , level3Proficient :: Float
  , level4Proficient :: Float
  , totalProficient :: Float
  }

instance Csv.FromNamedRecord MEAPScore where
  parseNamedRecord r =
    MEAPScore <$>
        (District <$> r .: "DistrictCode"
        <*> r .: "DistrictName")
      <*> r .: "Grade"
      <*> r .: "Subject Name"
      <*> r .: "Subgroup"
      <*> r .: "Number Tested"
      <*> r .: "Level 1 Proficient"
      <*> r .: "Level 2 Proficient"
      <*> r .: "Level 3 Proficient"
      <*> r .: "Level 4 Proficient"
      <*> r .: "Percent Proficient"

data DistrictLibrarians = DistrictLibrarians
  { district :: District
  , totalStaff :: Float
  , librarians :: Float
  }

getFileName :: Opt.Parser String
getFileName = argument str (metavar "file")

main :: IO ()
main = do
  fileName <- Opt.execParser opts
  csvData <- BL.readFile fileName
  case Csv.decodeByName csvData of
    Left err -> putStrLn err
    Right (_, v) -> return () -- returns vector of MEAPScores.
  where
    opts = Opt.info getFileName Opt.fullDesc
