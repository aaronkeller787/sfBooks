#####################################
#              IMPORTS              #
#####################################
import pandas as pd
import psycopg
import os
from info import pgUsername, dbName


# Read in just the 'Names' column
df = pd.read_csv("data/sf_military_new.csv", usecols=["Book_Title", "Author_Name"])

# Cleaner function
def cleanData(text):
  return (
        pd.Series(text)
        .astype(str)
        .str.replace(r"[^A-Za-z. ]", "", regex=True)  # allow letters, periods, spaces
        .str.replace(r"\s+", " ", regex=True)  # collapse multiple spaces
        .str.strip()
        .str.title()
    )

cleaned_df = df.apply(cleanData)

print(cleaned_df.head())

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
  pass