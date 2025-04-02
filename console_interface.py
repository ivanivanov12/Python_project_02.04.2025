import mysql.connector
from config import dbconfig_edit
from multilingual_search import translate_query


def get_connection():
    try:
        connection = mysql.connector.connect(**dbconfig_edit)
        return connection
    except mysql.connector.Error as err:
        print(f"Ошибка подключения: {err}")
        return None


def create_queries_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS queries (
                query_id INT AUTO_INCREMENT PRIMARY KEY,
                query_text VARCHAR(255),
                query_type ENUM('keyword', 'genre_year'),
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        connection.commit()
        print("Таблица 'queries' готова.")
    except mysql.connector.Error as err:
        print(f"Ошибка при создании таблицы: {err}")
    finally:
        cursor.close()


def save_query(connection, query_text, query_type):
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO queries (query_text, query_type)
            VALUES (%s, %s)
        """
        cursor.execute(query, (query_text, query_type))
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Ошибка при сохранении запроса: {err}")
    finally:
        cursor.close()


def search_movies_by_keyword(connection, keyword, target_language='en'):
    cursor = None
    try:
        translated_keyword = translate_query(keyword, target_language)
        if not translated_keyword:
            print("Не удалось перевести запрос.")
            return []

        cursor = connection.cursor()
        query = "SELECT film_id, title, description FROM film"
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            print("В базе данных нет данных для обработки.")
            return []

        keywords = [word.lower() for word in translated_keyword.split()]
        filtered_results = [
            (film_id, title) for film_id, title, description in results
            if all(kw in (title + " " + description).lower()
                   for kw in keywords)
        ]

        if not filtered_results:
            print("Фильмы не найдены. Запрос необходимо конкретизировать.")
        return filtered_results

    except mysql.connector.Error as err:
        print(f"Ошибка SQL: {err}")
        return []
    finally:
        if cursor:
            cursor.close()


def search_movies_by_genre_and_year(connection, genre, year):
    try:
        cursor = connection.cursor()
        query = """
            SELECT f.film_id, f.title
            FROM film f
            JOIN film_category fc ON f.film_id = fc.film_id
            JOIN category c ON fc.category_id = c.category_id
            WHERE c.name = %s AND f.release_year = %s
            LIMIT 10
        """
        cursor.execute(query, (genre, year))
        results = cursor.fetchall()
        return results
    except mysql.connector.Error as err:
        print(f"Ошибка SQL: {err}")
        return []
    finally:
        cursor.close()


def get_popular_queries(connection):
    try:
        cursor = connection.cursor()
        query = """
            SELECT query_text, COUNT(*) AS query_count
            FROM queries
            GROUP BY query_text
            ORDER BY query_count DESC
            LIMIT 10
        """
        cursor.execute(query)
        return cursor.fetchall()
    except mysql.connector.Error as err:
        print(f"Ошибка SQL: {err}")
        return []
    finally:
        cursor.close()


def get_statistics(connection):
    try:
        cursor = connection.cursor()

        total_queries_count = fetch_single_value(cursor, """
            SELECT COUNT(*) FROM queries
        """)
        print(f"\nОбщее количество запросов: {total_queries_count}")

        unique_queries_count = fetch_single_value(cursor, """
            SELECT COUNT(DISTINCT query_text) FROM queries
        """)
        print(f"Количество уникальных запросов: {unique_queries_count}")

        top_queries_results = fetch_all_results(cursor, """
            SELECT query_text, COUNT(*) AS query_count
            FROM queries
            GROUP BY query_text
            ORDER BY query_count DESC
            LIMIT 5
        """)
        print("\nТоп-5 популярных запросов:")
        for query_text, query_count in top_queries_results:
            print(f"{query_text}: {query_count}")

        genre_to_analyze = input("\nВведите жанр для анализа статистики "
                                 "запросов: ").lower()
        genre_queries_count = fetch_single_value(cursor, """
            SELECT COUNT(*) FROM queries WHERE query_text LIKE %s
        """, ('%' + genre_to_analyze + '%',))

        genre_percentage = (
            (genre_queries_count / total_queries_count) * 100
            if total_queries_count else 0
        )
        print(f"\nПроцент запросов, связанных с жанром '{genre_to_analyze}': "
              f"{genre_percentage:.2f}%")

    except mysql.connector.Error as err:
        print(f"Ошибка SQL при сборе статистики: {err}")
    finally:
        cursor.close()


def fetch_single_value(cursor, query, params=None):
    cursor.execute(query, params or ())
    return cursor.fetchone()[0]


def fetch_all_results(cursor, query, params=None):
    cursor.execute(query, params or ())
    return cursor.fetchall()


def main():
    connection = get_connection()
    if not connection:
        print("Не удалось подключиться к базе данных.")
        return
    create_queries_table(connection)
    while True:
        print("\nДобро пожаловать в систему поиска фильмов!")
        print("1. Найти фильмы по ключевому слову")
        print("2. Найти фильмы по жанру и году")
        print("3. Вывести самые популярные запросы")
        print("4. Показать статистику запросов")
        print("0. Выйти")
        action = input("Выберите действие: ")
        if action == "1":
            keyword = input("Введите ключевое слово для поиска: ")
            results = search_movies_by_keyword(connection, keyword)
            save_query(connection, keyword, "keyword")
            print("\nРезультаты поиска:")
            for film_id, title in results:
                print(f"ID: {film_id}, Название: {title}")
        elif action == "2":
            genre = input("Введите жанр: ")
            year = input("Введите год: ")
            results = search_movies_by_genre_and_year(connection, genre, year)
            query_text = f"{genre}, {year}"
            save_query(connection, query_text, "genre_year")
            print("\nРезультаты поиска:")
            for film_id, title in results:
                print(f"ID: {film_id}, Название: {title}")
        elif action == "3":
            results = get_popular_queries(connection)
            print("\nПопулярные запросы:")
            for query_text, query_count in results:
                print(f"Запрос: {query_text}, Количество: {query_count}")
        elif action == "4":
            get_statistics(connection)
        elif action == "0":
            print("Выход из программы.")
            break
        else:
            print("Некорректный ввод. Попробуйте снова.")
    connection.close()
    print("Соединение с базой данных закрыто.")


if __name__ == "__main__":
    main()
