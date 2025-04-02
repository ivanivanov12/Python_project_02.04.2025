import mysql.connector
from config import dbconfig_edit


def get_connection(db_config):
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except mysql.connector.Error as err:
        print(f"Ошибка подключения: {err}")
        return None


def database_exists(connection, db_name):
    try:
        cursor = connection.cursor()
        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall()]
        return db_name in databases
    finally:
        cursor.close()


def create_struct_database(db_config, db_name):
    connection = get_connection(db_config)
    if not connection:
        print("Ошибка подключения к базе данных.")
        return

    try:
        cursor = connection.cursor()

        if not database_exists(connection, db_name):
            cursor.execute(f"CREATE DATABASE {db_name}")
            print(f"База данных '{db_name}' успешно создана.")

        cursor.execute(f"USE {db_name}")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                firstname VARCHAR(50),
                lastname VARCHAR(50)
            )
        """)
        print("Таблица 'users' успешно создана.")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_name VARCHAR(100) NOT NULL,
                price DECIMAL(10, 2) NOT NULL
            )
        """)
        print("Таблица 'products' успешно создана.")
    except mysql.connector.Error as err:
        print(f"Ошибка при выполнении SQL: {err}")
    finally:
        cursor.close()
        connection.close()


create_struct_database(dbconfig_edit, 'my_project_db')
