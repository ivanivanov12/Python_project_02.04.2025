import mysql.connector
from config import dbconfig_read, dbconfig_edit


def get_connection(mode="edit"):
    try:
        if mode == "read":
            db_config = dbconfig_read
        elif mode == "edit":
            db_config = dbconfig_edit
        else:
            raise ValueError("Некорректный режим подключения. "
                             "Используйте 'read' или 'edit'.")

        connection = mysql.connector.connect(**db_config)
        return connection
    except mysql.connector.Error as err:
        print(f"Ошибка подключения: {err}")
        return None


def save_query(connection, film_id, query_text):
    try:
        cursor = connection.cursor()
        query = "INSERT INTO queries (film_id, query_text) VALUES (%s, %s)"
        cursor.execute(query, (film_id, query_text))
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Ошибка сохранения запроса: {err}")
    finally:
        if cursor:
            cursor.close()


def get_popular_movies(connection, limit=10):
    try:
        cursor = connection.cursor()
        query = """
        SELECT
            f.film_id, f.title, COALESCE(COUNT(q.query_id), 0) AS query_count
        FROM film f
        LEFT JOIN queries q ON f.film_id = q.film_id
        GROUP BY f.film_id, f.title
        ORDER BY query_count DESC
        LIMIT %s
        """
        cursor.execute(query, (limit,))
        results = cursor.fetchall()
        return results
    except mysql.connector.Error as err:
        print(f"Ошибка SQL: {err}")
        return []
    finally:
        if cursor:
            cursor.close()
