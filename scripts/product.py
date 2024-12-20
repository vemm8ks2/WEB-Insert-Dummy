import pandas as pd
from scripts.db_connection import create_connection
from mysql.connector import Error
import os
from dotenv import load_dotenv


load_dotenv()


save_product_query = """
    INSERT INTO products (title, price, image_url, active, category_id)
    VALUES (%s, %s, %s, true, %s);
"""

save_product_option_query = """
    INSERT INTO product_options (id, size, stock, product_id) 
    VALUES (%s, %s, %s, %s);
"""

find_products_seq_query = """
    SELECT next_val FROM product_options_seq LIMIT 1
"""

update_products_seq_query = """
    UPDATE product_options_seq SET next_val = %s
"""

find_product_by_title_query = """
    SELECT id FROM products WHERE title = %s;
"""

find_product_option_by_product_id_and_size_query = """
    SELECT id 
    FROM product_options 
    WHERE product_id = %s AND size = %s;
"""

update_product_option_quantity_by_id = """
    UPDATE product_options
    SET stock = stock + %s
    WHERE id = %s;
"""

def product_insert(product_csv_file, product_option_csv_file):
    connection = None
    cursor = None

    try:
        prod_cnt = 0
        opt_cnt = 0

        # 파일 읽기
        product_df = pd.read_csv(product_csv_file)
        option_df = pd.read_csv(product_option_csv_file)

        # DB 연결
        connection = create_connection()
        connection.start_transaction()
        cursor = connection.cursor()

        # Sequence 값 조회
        cursor.execute(find_products_seq_query)
        option_next_val = cursor.fetchone()[0]

        for _, product_row in product_df.iterrows():
            # 상품 조회
            cursor.execute(find_product_by_title_query, (product_row['상품명'],))
            has_product = cursor.fetchone()

            if has_product:
                # 상품이 이미 존재한다면 식별자만 추출
                product_id = has_product[0]
            else:
                # 상품이 존재하지 않는다면 저장
                image_url = f"{os.getenv("SUPABASE_URL")}/{os.getenv("SUPABASE_BUCKET_NAME")}/{product_row['이미지 URL']}"

                cursor.execute(save_product_query, (
                    product_row['상품명'],
                    product_row['가격'],
                    image_url,
                    product_row['카테고리 식별자']
                ))

                product_id = cursor.lastrowid

            # 상품명이 같은 옵션만 추출
            options = option_df[option_df['상품명'].str.match(product_row['상품명'], na=False)]

            for _, option_row in options.iterrows():
                # 옵션 조회
                cursor.execute(find_product_option_by_product_id_and_size_query, (
                    product_id,
                    option_row['사이즈']
                ))
                has_option = cursor.fetchone()

                if has_option:
                    # 옵션이 이미 존재한다면 재고량 추가
                    cursor.execute(update_product_option_quantity_by_id, (
                        option_row['재고'],
                        has_option[0]
                    ))
                else:
                    # 옵션이 존재하지 않는다면 저장
                    option_next_val += 1

                    cursor.execute(save_product_option_query, (
                        option_next_val,
                        option_row['사이즈'],
                        option_row['재고'],
                        product_id
                    ))

                opt_cnt += 1
            prod_cnt += 1

        # Sequence 값 업데이트
        cursor.execute(update_products_seq_query, (option_next_val,))

        connection.commit()
        print(f"{prod_cnt}개의 상품 데이터와 {opt_cnt}개의 옵션이 성공적으로 삽입 혹은 업데이트 되었습니다.")

    except Error as e:
        print(f"오류 발생: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL 연결 종료")
