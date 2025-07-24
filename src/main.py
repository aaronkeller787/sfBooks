#####################################
#              IMPORTS              #
#####################################            
import pandas as pd
import psycopg
from info import pgUsername, dbName

## What are the top 10 most popular books by rating or vote count, and who wrote them?
def question1():
    with psycopg.connect(f"dbname={dbName} user={pgUsername}") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    b.title AS book_title,
                    CONCAT(a.first_name, ' ', a.last_name) AS "Author",
                    b.rating,
                    b.total_votes,
                    year_published
                FROM
                    book_authors ba

                JOIN
                    books b ON ba.book_id = b.id
                JOIN
                    authors a ON ba.author_id = a.id
                ORDER BY total_votes DESC, rating DESC
                LIMIT 10;
            """)
            results = cur.fetchall()
            for row in results:
                print(f"{row[0]} by {row[1]} has a rating of {row[2]} with a total vote count of {row[3]}")

def question2():
        with psycopg.connect(f"dbname={dbName} user={pgUsername}") as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        CONCAT(a.first_name, ' ', a.last_name) AS author,
                        COUNT(*) AS book_count
                    FROM
                        book_authors ba
                    JOIN
                        authors a ON ba.author_id = a.id
                    GROUP BY
                        author
                    ORDER BY
                        book_count DESC
                        LIMIT 10;
                """)
                results = cur.fetchall()
                for row in results:
                    print(f"{row[0]} has written {row[1]} books")

def question3():
    with psycopg.connect(f"dbname={dbName} user={pgUsername}") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    year_published AS "publication_year",
                    AVG(total_votes)::NUMERIC(10,0) AS "vote_average"
                FROM books
                GROUP BY year_published
                ORDER BY year_published DESC;
                

            """)
            results = cur.fetchall()
            for row in results:
                print(f"Year {row[0]} had an average of {row[1]} votes")

if __name__ == "__main__":
    print("Question 1:" )
    print("What are the top 10 most popular books by rating or vote count, and who wrote them?")
    question1()

    print()
    
    print("Question 2:")
    print("Which authors have contributed to the most books?")
    question2()

    print()

    print("Question 3:")
    print("What are the average ratings and total votes per publication year?")
    question3()