import os.path

from scripts.order import order_insert
from scripts.product import product_insert
from scripts.user import user_insert


def has_file(path):
    is_file = os.path.isfile(path)

    if is_file:
        print("파일이 존재하므로 프로세스를 시작합니다.")
    else:
        print("파일이 존재하지 않습니다. 경로를 다시 입력해주세요.")

    return is_file


def main():
    while True:
        print("-" * 50)
        print("메뉴 목록:")
        print("1. 유저 정보 삽입")
        print("2. 상품 정보 삽입")
        print("3. 상품 정보 업데이트")
        print("4. 주문 정보 삽입")
        print("5. 프로그램 종료")
        print("-" * 50)

        try:
            opt = int(input("메뉴를 선택해주세요: "))

            if opt == 1:
                user_csv_path = input("유저 CSV 파일이 위치한 전체 경로를 기입해주세요: ")

                if has_file(user_csv_path):
                    user_insert(user_csv_path)
            elif opt == 2 or opt == 3:
                product_csv_path = input("상품 CSV 파일이 위치한 전체 경로를 기입해주세요: ")
                product_option_csv_path = input("상품 옵션 CSV 파일이 위치한 전체 경로를 기입해주세요: ")

                if has_file(product_csv_path) and has_file(product_option_csv_path):
                    product_insert(product_csv_path, product_option_csv_path)
            elif opt == 4:
                order_csv_path = input("주문 CSV 파일이 위치한 전체 경로를 기입해주세요: ")
                order_item_csv_path = input("주문 정보 CSV 파일이 위치한 전체 경로를 기입해주세요: ")

                if has_file(order_csv_path) and has_file(order_item_csv_path):
                    order_insert(order_csv_path, order_item_csv_path)
            elif opt == 5:
                print("프로그램을 종료합니다.")
                break
            else:
                print("메뉴 목록 중에 선택해주세요.")

        except ValueError:
            print("유효하지 않은 숫자입니다. 다시 입력해주세요.")

        finally:
            print("")


if __name__ == "__main__":
    main()
