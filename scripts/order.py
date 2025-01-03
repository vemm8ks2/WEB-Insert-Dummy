import pandas as pd
from scripts.db_connection import create_connection
from mysql.connector import Error
from dotenv import load_dotenv


load_dotenv()


find_user_id_by_username = """
    SELECT id FROM users WHERE username = %s;
"""

find_price_by_product_name_query = """
    SELECT id, price FROM products WHERE title = %s;
"""

save_order_query = """
    INSERT INTO orders (delivered_at, payment_method, shipping_address, total_price, receiver_name, receiver_phone, user_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

save_order_items_query = """
    INSERT INTO order_items (price, quantity, size, order_id, product_id)
    VALUES (%s, %s, %s, %s, %s);
"""

update_product_option_quantity_by_product_id_and_size = """
    UPDATE product_options
    SET stock = stock - %s
    WHERE product_id = %s AND size = %s;
"""


def order_insert(order_csv_file, order_item_csv_file, conn = None):
    connection = conn
    cursor = None

    try:
        order_cnt = 0
        item_cnt = 0

        # 파일 읽기
        order_df = pd.read_csv(order_csv_file)
        order_df = order_df.map(lambda v: None if pd.isna(v) else v)
        item_df = pd.read_csv(order_item_csv_file)

        # DB 연결
        if conn is None:
            connection = create_connection()
            connection.start_transaction()

        cursor = connection.cursor()

        for _, order_row in order_df.iterrows():
            total_price = 0

            # 상품 정보 추출
            items = item_df[item_df['주문 식별자'] == order_row['식별자']]
            item_values = []

            # 재고 정보 추출
            quantity_values = []

            # 유저 아이디 조회
            cursor.execute(find_user_id_by_username, (order_row['유저 아이디'],))
            user_id = cursor.fetchone()[0]

            # 상품 가격 계산
            for _, item_row in items.iterrows():
                cursor.execute(find_price_by_product_name_query, (item_row['상품명'],))
                product = cursor.fetchone()

                product_id = product[0]
                unit_price = product[1]
                total_price += item_row['수량'] * unit_price

                item_values.append((
                    unit_price,
                    item_row['수량'],
                    item_row['사이즈'],
                    None,
                    product_id
                ))

                quantity_values.append((item_row['수량'], product_id, item_row['사이즈']))

                item_cnt += 1

            # 주문 정보 삽입
            cursor.execute(save_order_query, (
                order_row['배송일'],
                order_row['결제 수단'],
                order_row['배송지'],
                total_price,
                order_row['수취인'],
                order_row['수취인 연락처'],
                user_id
            ))

            order_id = cursor.lastrowid

            item_values = [
                item[:3] + (order_id,) + item[4:] if item[3] is None else item
                for item in item_values
            ]

            # 주문 목록 정보 삽입
            cursor.executemany(save_order_items_query, item_values)

            # 재고 차감
            cursor.executemany(update_product_option_quantity_by_product_id_and_size, quantity_values)

            order_cnt += 1

        if conn is None:
            connection.commit()

        print(f"{order_cnt}개의 주문 데이터와 {item_cnt}개의 주문 목록이 성공적으로 삽입되었습니다.")

    except Error as e:
        print(f"오류 발생: {e}")
    finally:
        if conn is None and connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL 연결 종료")

def order_full_insert(order_full_path):
    conn = None

    try:
        conn = create_connection()
        conn.start_transaction()

        full_path_df = pd.read_csv(order_full_path)

        for _, full_path_row in full_path_df.iterrows():
            order_insert(full_path_row['주문 CSV'], full_path_row['주문 정보 CSV'], conn)

        conn.commit()

    except Error as e:
        print(f"오류 발생: {e}")
    finally:
        if  conn and conn.is_connected():
            conn.cursor().close()
            conn.close()
            print("MySQL 연결 종료")

    print("\n상품 및 상품 정보 일괄 삽입 완료")