import pandas as pd
from scripts.db_connection import create_connection
from mysql.connector import Error
import os
from dotenv import load_dotenv


load_dotenv()


def product_insert(product_csv_file, product_option_csv_file):
    connection = None
    cursor = None

    try:
        prod_cnt = 0
        opt_cnt = 0

        product_df = pd.read_csv(product_csv_file)
        option_df = pd.read_csv(product_option_csv_file)

        connection = create_connection()
        cursor = connection.cursor()

        product_insert_query = """
            INSERT INTO products (title, price, image_url, active, category_id)
            VALUES (%s, %s, %s, true, %s);
        """

        option_insert_query = """
            INSERT INTO product_options (id, size, stock, product_id) 
            VALUES (%s, %s, %s, %s);
        """

        cursor.execute("SELECT next_val FROM product_options_seq LIMIT 1")
        option_next_val = cursor.fetchone()[0]

        for _, product_row in product_df.iterrows():
            image_url = f"{os.getenv("SUPABASE_URL")}/{os.getenv("SUPABASE_BUCKET_NAME")}/{product_row['이미지 URL']}"

            cursor.execute(product_insert_query, (
                product_row['제목'],
                product_row['가격'],
                image_url,
                product_row['카테고리 식별자']
            ))

            product_id = cursor.lastrowid
            options = option_df[option_df['제목'].str.match(product_row['제목'], na=False)]

            for _, option_row in options.iterrows():
                option_next_val += 1

                cursor.execute(option_insert_query, (
                    option_next_val,
                    option_row['사이즈'],
                    option_row['재고'],
                    product_id
                ))

                opt_cnt += 1
            prod_cnt += 1

        cursor.execute("UPDATE product_options_seq SET next_val = %s", (option_next_val,))

        connection.commit()
        print(f"{prod_cnt}개의 상품 데이터와 {opt_cnt}개의 옵션이 성공적으로 삽입되었습니다.")

    except Error as e:
        print(f"오류 발생: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL 연결 종료")
