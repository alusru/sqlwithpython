import os
import mysql.connector
from mysql.connector import Error
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def mysql_connection(host, user, password):
    connection = None

    try:
        connection = mysql.connector.connect(
            host=host, user=user, password=password
        )
        print(f"database connected")
    except Error as err:
        print(f"Error: {err}")
    return connection


def create_db(connection, query):
    conn = connection.cursor()

    try:
        conn.execute(query)
        print("Database created")
    except Error as err:
        print(f"Error: {err}")


# connect to db
def db_connect(host, user, password, db):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=db
        )
        print("connected to database")
    except Error as err:
        print(f"Error: {err}")
    return connection


def exec_query(connection, query):
    try:
        conn = connection.cursor()
        conn.execute(query)
        connection.commit()
        print("Query was successful")
    except Error as err:
        print(f"Error: {err}")


if __name__ == '__main__':
    connect = mysql_connection(os.getenv('host'), os.getenv('user'), os.getenv('password'))

    # create database uncomment to run the first time
    create_db(connect, f"create database {os.getenv('db_name')}")

    con = db_connect(os.getenv('host'), os.getenv('user'), os.getenv('password'), os.getenv('db_name'))

    exec_query(
        con,
        """
    create table person(
    id int primary key,
    name varchar(30) not null,
    surname varchar(30) not null,
    date_of_birth date,
    age int,
    phone_number varchar(20)
    );
    """)

    insert_data = """
    insert into person values
    (1, 'kim', 'possible','1990-01-02',33,'0147896563'),
    (2, 'tom', 'possible','1991-01-02',32,'014789656343'),
    (3, 'tim', 'possible','1992-01-02',31,'0147896563245');
    """

    exec_query(con, insert_data)

