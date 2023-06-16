# Basic Table Creation and Connection Using the `psycopg2` and `sqlalchemy` Python Libraries

## PostgresSQL database project
#### Installing required packages

``` Python
!pip install psycopg2
!pip install sqlalchemy
```

### Importing necessary packages

``` Python
# Psycopg2 imports -- simple import lol
import psycopg2
# SQLAlchemy imports
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
import sqlalchemy
```

### Creating connections

``` Python
# Creating connections
try:
    # Very basic
    psyco_conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=password")

except psycopg2.Error as e:
    print("Error: Could not connect to database -- psyco")
    print(e)
    
try:
    # Create engine then create the raw connection so you can get a cursor
    engine = sqlalchemy.create_engine("postgresql://postgres:password@127.0.0.1:5432/postgres").execution_options(isolation_level="AUTOCOMMIT")
    alch_conn = engine.raw_connection()
except OperationalError as e:
    print("Error: Could not connect to database -- alch")
    print(e)

```

### Creating cursors


``` Python
try:
    # Create cursor
    alch_cursor = alch_conn.cursor()
except OperationalError as e:
    print("Error: Could not make a cursor -- alch")
    print(e)

try:
    # Create cursor -- pretty much the same from here on out I think...
    psyco_cursor = psyco_conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not create cursor -- psyco")
    print(e)

```

### Creating databases


``` Python
psyco_conn.set_session(autocommit=True)
try:
    # Commit so you don't get an error
    alch_cursor.execute("commit")
    # SQL query
    alch_cursor.execute("create database sqlalchdb")
    
except OperationalError as e:
    print("Error: Could not create database -- alch")
    print(e)

except Exception as e:
    print("Error: Could not create database -- alch")
    print(e)


try:
    # Same as alch process
    psyco_cursor.execute("commit")
    psyco_cursor.execute("create database psycodb")

except psycopg2.Error as e:
    print("Error: Could not create database -- psyco")
    print(e)
    

```

### Closing connections that have already been made and connecting to newly made DBs

``` Python 
alch_conn.close()
psyco_conn.close()

# Creating connections
try:
    # Very basic
    psyco_conn = psycopg2.connect("host=127.0.0.1 dbname=psycodb user=postgres password=password")

except psycopg2.Error as e:
    print("Error: Could not connect to database -- psyco")
    print(e)
    
try:
    # Create engine then create the raw connection so you can get a cursor
    engine = sqlalchemy.create_engine("postgresql://postgres:password@127.0.0.1:5432/sqlalchdb").execution_options(isolation_level="AUTOCOMMIT")
    alch_conn = engine.raw_connection()
except OperationalError as e:
    print("Error: Could not connect to database -- alch")
    print(e)
except Exception as e:
    print("Error: Could not connect to database -- alch")
    print(e)


```

### Recreating cursors


``` Python
try:
    # Create cursor
    alch_cursor = alch_conn.cursor()
except Exception as e:
    print("Error: Could not make a cursor -- alch")
    print(e)

try:
    # Create cursor -- pretty much the same from here on out I think...
    psyco_cursor = psyco_conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not create cursor -- psyco")
    print(e)

```

### Create table with the following columns
| student_id | name | age | gender | subject | marks |
|--------|----------|---------|----|---------|------|
|  |      |     |        |        |       |


``` Python
# Set auto commit to true
psyco_conn.set_session(autocommit=True)
alch_conn.set_session(autocommit=True)

```

``` Python
# Creating tables
try:
    alch_cursor.execute("CREATE TABLE IF NOT EXISTS students(student_id int primary key, name varchar(50),\
    age int, gender varchar(1), subject varchar(50), marks varchar(5))")
    
except Exception as e:
    print("Could not make table students -- alch")
    print(e)

try:
    psyco_cursor.execute("CREATE TABLE IF NOT EXISTS students(student_id int primary key, name varchar(50),\
    age int, gender varchar(1), subject varchar(50), marks varchar(5))")
    
except psycopg2.Error as e:
    print("Could not make table students -- psyco")
    print(e)



```

### Inserting data into tables


``` Python
# Inserting into sqlalchdb...
try:
    alch_cursor.execute("INSERT INTO students (student_id, name, age, gender, subject, marks)\
                         VALUES (1, 'Bob', 24, 'M', 'Python', 86)")
except Exception as e:
    print("Error: Could not insert into table students")
    print(e)

try:
    alch_cursor.execute("INSERT INTO students (student_id, name, age, gender, subject, marks)\
                         VALUES (2, 'Pablo', 23, 'M', 'C#', 82)")
except Exception as e:
    print("Error: Could not insert into table students")
    print(e)

    
```

``` Python
# Inserting into psycodb...
try:
    psyco_cursor.execute("INSERT INTO students (student_id, name, age, gender, subject, marks)\
                          VALUES (2, 'Pablo', 23, 'M', 'C#', 82)")
except psycopg2.Error as e:
    print("Error: Could not insert into table students")
    print(e)

try:
    psyco_cursor.execute("INSERT INTO students (student_id, name, age, gender, subject, marks)\
                          VALUES (1, 'Bob', 24, 'M', 'Python', 86)")
except psycopg2.Error as e:
    print("Error: Could not insert into table students")
    print(e)

```


### Seeing the additions

``` Python
# Using select psyco
try:
    psyco_cursor.execute("SELECT * FROM students")
    
except psycopg2.Error as e:
    print("Error: Could not select anything from students -- psyco")
    print(e)

psyco_row = psyco_cursor.fetchone()
print("Psycopg2")
while psyco_row:
    print(psyco_row)
    psyco_row = psyco_cursor.fetchone()

print("\n---------\n")

# Using select sqlalch
try:
    alch_cursor.execute("SELECT * FROM students")
except Exception as e:
    print("Error: Could not select anything from students -- alch")

alch_row = alch_cursor.fetchone()
print("SQLAlchemy")
while alch_row:
    print(alch_row)
    alch_row = alch_cursor.fetchone()
```

### Closing cursor and connection


``` Python
psyco_cursor.close()
alch_cursor.close()
psyco_conn.close()
alch_conn.close()
```

# Extracting Mental Health Data and Exporting to Local PostgreSQL Server

## Extracting Mental Health Data to PostgreSQL
### Import Packages

``` Python
import psycopg2
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
## Connect to DB in SQLite and Make a Pandas DataFrame
# SQLite connection
sqliteConnection = sqlite3.connect("Data/mental_health.sqlite")

# Run the commented out section to see the tables
# sql_query = """SELECT name FROM sqlite_master  
#   WHERE type='table';"""

# sqlite_cursor = sqliteConnection.cursor()
# sqlite_cursor.execute(sql_query)
# print(sqlite_cursor.fetchall())

# Pandas DF
pd_answer_db = pd.read_sql("SELECT * FROM Answer", sqliteConnection)
pd_question_db = pd.read_sql("SELECT * FROM Question", sqliteConnection)
pd_survey_db = pd.read_sql("SELECT * FROM Survey", sqliteConnection) 
```

### Structure of Data

``` Python
print(pd_answer_db.head())
print("-------------------")
print(pd_question_db.head())
print("-------------------")
print(pd_survey_db.head())
## Get General Information About Data
print(pd_answer_db.describe())
print("-"*50)
print(pd_answer_db.info())
print("\n"+ "-"*100 + "\n")

print(pd_question_db.describe())
print("-"*50)
print(pd_question_db.info())
print("\n"+ "-"*100 + "\n")

print(pd_survey_db.describe())
print("-"*50)
print(pd_survey_db.info())
print("\n"+ "-"*100 + "\n")
```

## Get Connection with Local PostgreSQL Database and Make Cursor

``` Python
# Make the connection and set autocommit on
psycoConnection = psycopg2.connect("dbname=postgres user=postgres password=password")
psycoConnection.set_session(autocommit=True)

# Make the cursor
psycoCursor = psycoConnection.cursor()
```

## Create Database and Tables with Appropriate Columns
### Create Database and Create New Connection and Cursor

``` python
try:
    psycoCursor.execute("CREATE DATABASE mental_health_data;")
except psycopg2.Error as err:
    print("Error: Could not create database")
    print(err)

psycoCursor.close()
psycoConnection.close()

psycoConnection = psycopg2.connect("dbname=mental_health_data user=postgres password=password")
psycoConnection.set_session(autocommit=True)
psycoCursor = psycoConnection.cursor()

# Query for answers table

answers_query = """CREATE TABLE IF NOT EXISTS answers 
                    (AnswerText text not null,
                     SurveyID int ,
                     UserID int not null,
                     QuestionID int,
                     CONSTRAINT fk_surveyid
                       FOREIGN KEY(SurveyID)
                         REFERENCES surveys(SurveyID),
                     CONSTRAINT fk_questionid
                       FOREIGN KEY(QuestionID)
                         REFERENCES questions(questionid)
                    );
                      """    

# Execute querise to create tables
psycoCursor.execute("CREATE TABLE IF NOT EXISTS surveys (SurveyID int not null primary key, Description varchar(100) not null);")
psycoCursor.execute("CREATE TABLE IF NOT EXISTS questions (questiontext text not null, questionid int not null primary key);")
psycoCursor.execute(answers_query)

```

## Pull Data from pandas DF and Place Into Local PostgreSQL Database

``` Python
conn_string = "postgresql://postgres:password@127.0.0.1:5432/mental_health_data"
engine = create_engine(conn_string)
conn = engine.connect()

# Survey table
pd_survey_db.columns = ["surveyid", "description"]
pd_survey_db.to_sql("surveys", con=conn, if_exists='append', index=False)

# Question table
pd_question_db.columns = ["questiontext", "questionid"]
pd_question_db.to_sql("questions", con=conn, if_exists='append', index=False)

# Answer table
pd_answer_db.columns = ['answertext', 'surveyid', 'userid', 'questionid']
pd_answer_db.to_sql("answers", con=conn, if_exists='append', index=False)
psycoConnection.close()
psycoCursor.close()
conn.close()
```

