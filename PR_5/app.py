from flask import Flask
from flask_smorest import Api
from flask_jwt_extended import JWTManager
from db import db
from resources.user import user_blueprint
import psycopg2
import time
import random

# Flask application setup
def create_app():
    app = Flask(__name__)

    db_config = {
        "dbname": "Test_DB",
        "user": "Test_DB_owner",
        "password": "qfwFXR5bcvG9",
        "host": "ep-restless-sun-a598onk6.us-east-2.aws.neon.tech",
        "port": 5432
    }

    def execute_query(connection, query, data=None):
        with connection.cursor() as cursor:
            if data:
                cursor.execute(query, data)
            else:
                cursor.execute(query)
            connection.commit()

    def batch_insert(connection, query, data, batch_size=10000):
        with connection.cursor() as cursor:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                args_str = ','.join(cursor.mogrify("(%s)", x).decode('utf-8') for x in batch)
                cursor.execute(query + args_str)
            connection.commit()

    def measure_query_time(connection, query, data=None):
        start_time = time.time()
        execute_query(connection, query, data)
        end_time = time.time()
        return end_time - start_time

    connection = psycopg2.connect(
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"],
        sslmode="require"
    )

    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            data TEXT
        );
        """
        execute_query(connection, create_table_query)

        record_counts = [1000, 10000, 100000, 1000000]
        results = []

        for count in record_counts:
            print(f"\nTesting with {count} records...")

            data = [(f"Sample data {i}",) for i in range(count)]
            insert_query = "INSERT INTO test_table (data) VALUES "

            start_time = time.time()
            batch_insert(connection, insert_query, data)
            insert_time = time.time() - start_time

            select_query = "SELECT * FROM test_table;"
            select_time = measure_query_time(connection, select_query)

            update_query = "UPDATE test_table SET data = 'Updated data' WHERE id = %s;"
            update_time = measure_query_time(connection, update_query, (random.randint(1, count),))

            delete_query = "DELETE FROM test_table WHERE id = %s;"
            delete_time = measure_query_time(connection, delete_query, (random.randint(1, count),))

            results.append({
                "records": count,
                "insert_time": insert_time,
                "select_time": select_time,
                "update_time": update_time,
                "delete_time": delete_time
            })

            execute_query(connection, "TRUNCATE TABLE test_table;")

        print("\nPerformance Results:")
        for result in results:
            print(result)

    finally:
        connection.close()

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
