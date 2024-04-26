import mysql.connector
from mysql.connector import Error

def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host='sql_container',  # Name of the MySQL container
            user='de300_u1',  # As defined in the MySQL Docker container
            password='de300u1springaccess',  # As defined in the MySQL Docker container
            database='mysql'  # Default database or replace with your database name
        )
        if connection.is_connected():
            db_info = connection.get_server_info()
            print("Successfully connected to MySQL server version ", db_info)
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Test the connection
connect_to_mysql()
