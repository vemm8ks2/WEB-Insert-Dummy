from scripts.user_data_control import insert


def main():
    csv_file = 'data/user.csv'
    insert(csv_file)


if __name__ == "__main__":
    main()
