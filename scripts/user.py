import pandas as pd
from scripts.db_connection import create_connection
from mysql.connector import Error


save_users_query = """
    INSERT INTO users (id, username, password, gender, role, birth_date, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
"""


def user_insert(csv_file):
    connection = None
    cursor = None

    try:
        # 파일 읽기
        df = pd.read_csv(csv_file)

        # DB 연결
        connection = create_connection()
        connection.start_transaction()
        cursor = connection.cursor()

        # Sequence 값 조회
        cursor.execute("SELECT next_val FROM users_seq LIMIT 1")
        user_next_val = cursor.fetchone()[0]

        user_values = []

        for _, row in df.iterrows():
            user_values.append((
                user_next_val,  # id는 next_val로 설정
                row['아이디'],
                row['비밀번호'],
                row['성별'],
                row['역할'],
                row['생일'],
                row['생성날짜']
            ))

            user_next_val += 1

        # Sequence 값 업데이트
        cursor.execute("UPDATE users_seq SET next_val = %s", (user_next_val,))

        # 유저 저장
        cursor.executemany(save_users_query, user_values)

        connection.commit()
        print(f"{len(user_values)}개의 데이터가 성공적으로 삽입되었습니다.")

    except Error as e:
        print(f"오류 발생: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL 연결 종료")
