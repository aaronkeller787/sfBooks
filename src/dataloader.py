#####################################
#              IMPORTS              #
#####################################
import pandas as pd
import psycopg
import os
from glob import glob
from info import pgUsername, dbName

## Columns to be used
columns = ["Book_Title", "Author_Name", "Year_published", 
           "Rating_score", "Rating_votes", "Book_Description"]

## Base directory
directory = "data/"

## Looking in the data directory for all .csv files
def readFiles(directory, columns):
    csv_files = glob(os.path.join(directory, "*.csv"))
    dfs = [] ## Empty list that will be used to store the Dataframe
    for file in csv_files:
        print(f"Reading {file} ...")  ## Used for testing to confirm file names
        df = pd.read_csv(file, usecols=columns) ## Reads the CSVs
        dfs.append(df)  ## Appends the read CSV data to a large Dataframe
    combined_df = pd.concat(dfs, ignore_index=True) 
    return combined_df

## Function used to clean the Book_Title column
def cleanTitles(df):
  df["Book_Title"] = (
    df["Book_Title"]
    .str.replace(r"[^A-Za-z. ]", "", regex=True)  ## Allows for Letters, Periods, and Spaces
    .str.replace(r"\s+", " ", regex=True)  ## Removes multiple spaces
    .str.strip()  ## Removes white spaces before/after the text
    .str.title()  ## Capatilizes the first character of each string
  )
  return df

## Function used to clean the Author_Names column
def cleanAuthors(df):
  df["Author_Name"] = (
    df["Author_Name"]
    .str.replace(r"[^A-Za-z. ]", "", regex=True)  ## Allows for Letters, Periods, and Spaces
    .str.replace(r"\s+", " ", regex=True)  ## Removes multiple spaces
    .str.strip()  ## Removes white spaces before/after the text
    .str.title()  ## Capatilizes the first character of each string
  )
  return df

## Loads Books data from the Dataframe into the books table
def loadBooks(df):
    with psycopg.connect(f"dbname={dbName} user={pgUsername}") as conn:
        with conn.cursor() as cur:
            for row in df.itertuples(index=False):
                cur.execute("""
                    INSERT INTO books (title, rating, total_votes, description, year_published)
                    VALUES (%s, %s, %s, %s, %s) ON CONFLICT (title) DO NOTHING
                """, (
                    row.Book_Title,         ## Book_Title row from the Dataframe
                    row.Rating_score,       ## BRating_score row from the Dataframe
                    row.Rating_votes,       ## Rating_votes row from the Dataframe
                    row.Book_Description,   ## Book_Description row from the Dataframe
                    row.Year_published      ## Year_published row from the Dataframe
                ))
        conn.commit()


if __name__ == "__main__":

  all_books_df = readFiles(directory, columns)
  all_books_df = cleanTitles(all_books_df)   # Clean titles and reassign
  all_books_df = cleanAuthors(all_books_df)  # Clean authors and reassign
  loadBooks(all_books_df) 