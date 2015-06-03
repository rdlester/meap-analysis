#!/usr/bin/env python3

import csv
from enum import Enum
import functools
import sqlite3

class RowType(Enum):
  score = 1
  staff = 2

class District(object):
  @staticmethod
  def createTable(c):
    c.execute('''CREATE TABLE IF NOT EXISTS district
        (id INTEGER, name TEXT, PRIMARY KEY(id ASC));''')

  def __init__(self):
    self.hasData = False

  def parseCsvRowOfType(self, csvRow, rowType):
    if rowType == RowType.score:
      self.did = int(csvRow[3]) if csvRow[3] != '' else 0
      self.name = csvRow[4]
      self.hasData = True
    elif rowType == RowType.staff:
      self.did = int(csvRow[0]) if csvRow[0] != '' else 0
      self.name = csvRow[1]
      self.hasData = True

  def insertToSQL(self, c):
    if self.hasData:
      try:
        c.execute("INSERT INTO district VALUES (?, ?);", (self.did, self.name))
      except sqlite3.IntegrityError:
        return # no-op

class Score(object):
  rowid = 1

  @staticmethod
  def createTable(c):
    c.execute('''CREATE TABLE IF NOT EXISTS meap_score
        (district_id INTEGER NOT NULL, grade INTEGER NOT NULL, subject VARCHAR NOT NULL,
        subgroup VARCHAR NOT NULL, num_tested INTEGER NOT NULL, level1_proficient REAL NOT NULL,
        level2_proficient REAL NOT NULL, level3_proficient REAL NOT NULL, level4_proficient REAL NOT NULL,
        total_proficient REAL NOT NULL, avg_score REAL NOT NULL, std_dev REAL NOT NULL,
        id INTEGER, PRIMARY KEY(id ASC), FOREIGN KEY (district_id) REFERENCES district(id));''')

  def __init__(self):
    self.hasData = False
    return

  @staticmethod
  def verifyCsvRow(csvRow):
    # must have a district.
    did = csvRow[3]
    if did == '':
      return False

    # not associated with a specific building
    bid = csvRow[5]
    if bid != '':
      return False

    # enough students for data to be reported
    numTested = csvRow[10]
    if numTested == '< 10':
      return False

    return True

  @staticmethod
  def parseProficiencyScore(scoreString):
    if scoreString == '':
      return 0
    elif scoreString == '< 5%':
      return 5
    elif scoreString == '> 95%':
      return 95
    else:
      return float(scoreString)

  def parseCsvRow(self, csvRow):
    if Score.verifyCsvRow(csvRow):
      self.did = int(csvRow[3]) if csvRow[3] != '' else 0
      self.grade = int(csvRow[7]) if csvRow[7] != '' else 0
      self.subject = csvRow[8]
      self.subgroup = csvRow[9]
      self.numTested = int(csvRow[10]) if csvRow[10] != '' else 0
      self.l1 = Score.parseProficiencyScore(csvRow[11])
      self.l2 = Score.parseProficiencyScore(csvRow[12])
      self.l3 = Score.parseProficiencyScore(csvRow[13])
      self.l4 = Score.parseProficiencyScore(csvRow[14])
      self.total = Score.parseProficiencyScore(csvRow[15])
      self.avgScore = float(csvRow[16])
      self.stdDev = float(csvRow[17])
      self.rowid = Score.rowid
      Score.rowid += 1
      self.hasData = True

  def insertToSQL(self, c):
    if self.hasData:
      c.execute('''INSERT INTO meap_score VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);''',
          (self.did, self.grade, self.subject, self.subgroup,
              self.numTested, self.l1, self.l2, self.l3,
              self.l4, self.total, self.avgScore, self.stdDev, self.rowid))

class Staff(object):
  rowid = 1

  @staticmethod
  def createTable(c):
    c.execute('''CREATE TABLE IF NOT EXISTS school_staff
        (district_id INTEGER NOT NULL, teachers REAL NOT NULL, librarians REAL NOT NULL,
        library_support REAL NOT NULL, id INTEGER, PRIMARY KEY(id ASC),
        FOREIGN KEY (district_id) REFERENCES district(id));''')

  def __init__(self):
    self.hasData = False

  def parseCsvRow(self, csvRow):
    self.did = int(csvRow[0])
    self.teachers = float(csvRow[2]) if csvRow[2] != '' else 0
    self.librarians = float(csvRow[6]) if csvRow[6] != '' else 0
    self.library_support = float(csvRow[7]) if csvRow[7] != '' else 0
    self.rowid = Staff.rowid
    Staff.rowid += 1
    self.hasData = True

  def insertToSQL(self, c):
    if self.hasData:
      c.execute('''INSERT INTO school_staff VALUES
          (?, ?, ?, ?, ?);''',
          (self.did, self.teachers, self.librarians, self.library_support, self.rowid))

def makeDatabase():
  conn = sqlite3.connect('py-meap.db')
  c = conn.cursor()

  # Make tables
  District.createTable(c)
  Score.createTable(c)
  Staff.createTable(c)

  with open('csv/12-13.csv') as f:
    freader = csv.reader(f)
    freader.__next__() # skip header
    for row in freader:
      dobj = District()
      dobj.parseCsvRowOfType(row, RowType.score)
      dobj.insertToSQL(c)
      sobj = Score()
      sobj.parseCsvRow(row)
      sobj.insertToSQL(c)

  with open('csv/librarians-per-district.csv') as f:
    freader = csv.reader(f)
    freader.__next__() # skip header
    for row in freader:
      dobj = District()
      dobj.parseCsvRowOfType(row, RowType.staff)
      dobj.insertToSQL(c)
      sobj = Staff()
      sobj.parseCsvRow(row)
      sobj.insertToSQL(c)

  conn.commit()
  conn.close()

if __name__ == "__main__":
  makeDatabase()
