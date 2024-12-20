import mysql.connector
import json
from dotenv import load_dotenv
import os


# MySQL 데이터베이스에 연결하는 함수
def create_connection():
    connection = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),  # 포트 정보 추가
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DRIVER_CLASS_NAME")
    )
    return connection
