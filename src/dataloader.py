#####################################
#              IMPORTS              #
#####################################
import pandas as pd
import psycopg
import os
from info import pgUsername, dbName


def readAndCleanData():

  # Read in just the 'Names' column
  df = pd.read_csv("data/sf_military_new.csv", usecols=["Author_Name"])

# Clean the names
  cleaned_series = (
      df["Author_Name"]
        .str.replace(r"[^A-Za-z ]", "", regex=True)  # remove punctuation/digits
        .str.split()                                 # split into words
        .apply(" ".join)                             # rejoin words with single spaces
        .str.title()                                 # capitalize each word
  )

  # Convert to a list if needed
  cleaned_names = cleaned_series.tolist()

  print(cleaned_names)



def transformData():
  pass


# Testing DB connection and querying test DB
def loadData():
    
    with psycopg.connect(f"dbname={dbName} user={pgUsername}") as conn:
      with conn.cursor() as cur:
        cur.execute("SELECT * FROM test")
        results = cur.fetchall()
        for row in results:
          print(row)


if __name__ == "__main__":
    readAndCleanData()
    #loadData()