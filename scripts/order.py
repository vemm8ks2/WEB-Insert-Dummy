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

find_orders_seq_query = """
    SELECT next_val FROM orders_seq LIMIT 1;
"""

update_orders_seq_query = """
    UPDATE orders_seq SET next_val = %s;
"""

find_order_items_seq_query = """
    SELECT next_val FROM order_items_seq LIMIT 1;
"""

update_orders_items_seq_query = """
    UPDATE order_items_seq SET next_val = %s;
"""

save_order_query = """
    INSERT INTO orders (id, delivered_at, payment_method, shipping_address, total_price, receiver_name, receiver_phone, user_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
"""

save_order_items_query = """
    INSERT INTO order_items (id, price, quantity, size, order_id, product_id)
    VALUES (%s, %s, %s, %s, %s, %s);
"""


def order_insert(order_csv_file, order_item_csv_file):
    connection = None
    cursor = None

    try:
        order_cnt = 0
        item_cnt = 0

        # 파일 읽기
        order_df = pd.read_csv(order_csv_file)
        order_df = order_df.map(lambda v: None if pd.isna(v) else v)
        item_df = pd.read_csv(order_item_csv_file)

        # DB 연결
        connection = create_connection()
        connection.start_transaction()
        cursor = connection.cursor()

        # Sequence 값 조회
        cursor.execute(find_order_items_seq_query)
        item_next_val = cursor.fetchone()[0]

        cursor.execute(find_orders_seq_query)
        order_next_val = cursor.fetchone()[0]

        for _, order_row in order_df.iterrows():
            order_next_val += 1
            total_price = 0

            # 상품 정보 추출
            items = item_df[item_df['주문 식별자'] == order_row['식별자']]
            item_values = []

            # 유저 아이디 조회
            cursor.execute(find_user_id_by_username, (order_row['유저 아이디'],))
            user_id = cursor.fetchone()[0]

            # 상품 가격 계산
            for _, item_row in items.iterrows():
                item_next_val += 1

                cursor.execute(find_price_by_product_name_query, (item_row['상품명'],))
                product = cursor.fetchone()

                product_id = product[0]
                unit_price = product[1]
                total_price += item_row['수량'] * unit_price

                item_values.append((
                    item_next_val,
                    unit_price,
                    item_row['수량'],
                    item_row['사이즈'],
                    None,
                    product_id
                ))

                item_cnt += 1

            # 주문 정보 삽입
            cursor.execute(save_order_query, (
                order_next_val,
                order_row['배송일'],
                order_row['결제 수단'],
                order_row['배송지'],
                total_price,
                order_row['수취인'],
                order_row['수취인 연락처'],
                user_id
            ))

            item_values = [
                item[:4] + (order_next_val,) + item[5:] if item[4] is None else item
                for item in item_values
            ]

            # 주문 목록 정보 삽입
            cursor.executemany(save_order_items_query, item_values)
            order_cnt += 1


        # Sequence 값 업데이트
        cursor.execute(update_orders_seq_query, (order_next_val,))
        cursor.execute(update_orders_items_seq_query, (item_next_val,))

        connection.commit()
        print(f"{order_cnt}개의 주문 데이터와 {item_cnt}개의 주문 목록이 성공적으로 삽입되었습니다.")

    except Error as e:
        print(f"오류 발생: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("MySQL 연결 종료")
