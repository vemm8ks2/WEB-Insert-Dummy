import pandas as pd
from scripts.db_connection import create_connection
from mysql.connector import Error


def user_insert(csv_file):
    connection = None
    cursor = None

    try:
        df = pd.read_csv(csv_file)
        connection = create_connection()
        cursor = connection.cursor()

        cursor.execute("SELECT next_val FROM users_seq LIMIT 1")
        next_val = cursor.fetchone()[0]

        values = []

        for _, row in df.iterrows():
            # next_val 값을 증가시킴
            next_val += 1

            values.append((
                next_val,  # id는 next_val로 설정
                row['아이디'],
                row['비밀번호'],
                row['성별'],
                row['역할'],
                row['생일'],
                row['생성날짜']
            ))

        query = """
            INSERT INTO users (id, username, password, gender, role, birth_date, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """

        cursor.execute("UPDATE users_seq SET next_val = %s", (next_val,))
        cursor.executemany(query, values)

        connection.commit()
        print(f"{len(values)}개의 데이터가 성공적으로 삽입되었습니다.")

    except Error as e:
        print(f"오류 발생: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL 연결 종료")
