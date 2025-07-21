#####################################
#              IMPORTS              #
#####################################
import pandas as pd
import psycopg
import os
from info import pgUsername, dbName


def extractData():
  pass


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
    #extractData()
    loadData()