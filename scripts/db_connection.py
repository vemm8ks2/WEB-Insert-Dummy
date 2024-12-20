import mysql.connector
import os
from dotenv import load_dotenv


load_dotenv()


# MySQL 데이터베이스에 연결하는 함수
def create_connection():
    connection = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    return connection
