# sfBooks
This project is meant to extract data from external sources, transform it into usable/sanatized data, and store that information inside of a PostgreSQL Database.

## 🚀 Project Approach
📚 1. Information Gathering

    Explored multiple public datasets to find one suitable for a genre-specific database project.
    Selected the Kaggle Science Fiction Books dataset, which aggregates data from Goodreads — including fields such as title, author, rating, number of ratings, series info, and more.

      -  https://www.kaggle.com/datasets/tanguypledel/science-fiction-books-subgenres/discussion/403410

## 🧩 2. Design

    Analyzed the dataset and mapped out the relational database schema:
    - Identified core entities: books, authors, ratings, etc.
    - Created junction tables to support many-to-many relationships (e.g., book_author).

    Developed a list of real-world questions the database should be able to answer:
        1. What are the most popular books by rating or vote count, and who wrote them?
        2. Which authors have contributed to the most books?
        3. What are the average ratings and total votes per publication year?

## 🛠️ 3. Build

    Implemented the schema using PostgreSQL, enforcing normalization, primary/foreign keys, and constraints.
    Developed a Python-based ETL pipeline to process and insert data into the database.

## 🔧 Python Libraries Used

    pandas — for reading, cleaning, and transforming the dataset.
    psycopg — for database connectivity and executing SQL commands from Python.
