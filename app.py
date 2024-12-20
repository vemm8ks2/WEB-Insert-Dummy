from scripts.product_data_control import product_insert
from scripts.user_data_control import user_insert


def main():
    #csv_file = 'data/user.csv'
    #user_insert(csv_file)

    prod_csv = 'data/product.csv'
    prod_opt_csv = 'data/product_options.csv'

    product_insert(prod_csv, prod_opt_csv)


if __name__ == "__main__":
    main()
