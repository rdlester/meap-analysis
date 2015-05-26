#!/usr/bin/env python3

import csv
from enum import Enum
import sqlite3

class RowType(Enum):
  score = 1
  staff = 2

class District(object):
  def __init__(self, csvRow, rowType):
    if rowType == RowType.score:
      self.did = int(csvRow[3]) if csvRow[3] != '' else 0
      self.name = csvRow[4]
    elif rowType == RowType.staff:
      self.did = int(csvRow[0]) if csvRow[0] != '' else 0
      self.name = csvRow[1]

  def insertToSQL(self, c):
    try:
      c.execute("INSERT INTO district VALUES (?, ?)", (self.did, self.name))
    except sqlite3.IntegrityError:
      return # no-op

class Score(object):
  rowid = 1

  def __init__(self, csvRow):
    self.did = int(csvRow[3]) if csvRow[3] != '' else 0
    self.grade = int(csvRow[7]) if csvRow[7] != '' else 0
    self.subject = csvRow[8]
    self.subgroup = csvRow[9]
    self.numTested = int(csvRow[10]) if csvRow[10] != '' else 0
    self.l1 = float(csvRow[11]) if csvRow[11] != '' else 0
    self.l2 = float(csvRow[12]) if csvRow[12] != '' else 0
    self.l3 = float(csvRow[13]) if csvRow[13] != '' else 0
    self.l4 = float(csvRow[14]) if csvRow[14] != '' else 0
    self.total = float(csvRow[15]) if csvRow[15] != '' else 0
    self.rowid = Score.rowid
    Score.rowid += 1

  def insertToSQL(self, c):
      c.execute('''INSERT INTO meap_score VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
          (self.did, self.grade, self.subject, self.subgroup,
              self.numTested, self.l1, self.l2, self.l3,
              self.l4, self.total, self.rowid))

class Staff(object):
  rowid = 1

  def __init__(self, csvRow):
    self.did = int(csvRow[0])
    self.teachers = float(csvRow[2]) if csvRow[2] != '' else 0
    self.librarians = float(csvRow[6]) if csvRow[6] != '' else 0
    self.library_support = float(csvRow[7]) if csvRow[7] != '' else 0
    self.rowid = Staff.rowid
    Staff.rowid += 1

  def insertToSQL(self, c):
    c.execute('''INSERT INTO school_staff VALUES
        (?, ?, ?, ?, ?)''',
        (self.did, self.teachers, self.librarians, self.library_support, self.rowid))

def makeDatabase():
  conn = sqlite3.connect('py-meap.db')
  c = conn.cursor()

  # Make tables
  c.execute('''CREATE TABLE district
      (id INTEGER, name TEXT, PRIMARY KEY(id ASC))''')
  c.execute('''CREATE TABLE meap_score
      (district_id INTEGER NOT NULL, grade INTEGER NOT NULL, subject VARCHAR NOT NULL,
      subgroup VARCHAR NOT NULL, num_tested INTEGER NOT NULL, level1_proficient REAL NOT NULL,
      level2_proficient REAL NOT NULL, level3_proficient REAL NOT NULL, level4_proficient REAL NOT NULL,
      total_proficient REAL NOT NULL, id INTEGER, PRIMARY KEY(id ASC),
      FOREIGN KEY (district_id) REFERENCES district(id))''')
  c.execute('''CREATE TABLE school_staff
      (district_id INTEGER NOT NULL, teachers REAL NOT NULL, librarians REAL NOT NULL,
      library_support REAL NOT NULL, id INTEGER, PRIMARY KEY(id ASC),
      FOREIGN KEY (district_id) REFERENCES district(id))''')

  with open('csv/12-13_clean2.csv') as f:
    freader = csv.reader(f)
    freader.__next__() # skip header
    for row in freader:
      dobj = District(row, RowType.score)
      dobj.insertToSQL(c)
      sobj = Score(row)
      sobj.insertToSQL(c)

  with open('csv/librarians-per-district.csv') as f:
    freader = csv.reader(f)
    freader.__next__() # skip header
    for row in freader:
      dobj = District(row, RowType.staff)
      dobj.insertToSQL(c)
      sobj = Staff(row)
      sobj.insertToSQL(c)

  conn.commit()
  conn.close()

if __name__ == "__main__":
  makeDatabase()
