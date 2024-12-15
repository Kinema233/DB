import psycopg2
import time
from psycopg2.extras import execute_values

DB_CONFIG = {
    "dbname": "DB_Tests",
    "user": "DB_Tests_owner",
    "password": "e4xZpR7jazwW",
    "host": "ep-still-water-a5deoor3.us-east-2.aws.neon.tech",
    "port": 5432
}

def batch_insert(connection, query, data, batch_size=10000):
    with connection.cursor() as cursor:
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            execute_values(cursor, query, batch)
    connection.commit()

connection = psycopg2.connect(
    dbname=DB_CONFIG["dbname"],
    user=DB_CONFIG["user"],
    password=DB_CONFIG["password"],
    host=DB_CONFIG["host"],
    port=DB_CONFIG["port"],
    sslmode="require"
)

try:
    create_table_query = """
    CREATE TABLE IF NOT EXISTS test_table (
        id SERIAL PRIMARY KEY,
        data TEXT
    );
    """
    with connection.cursor() as cursor:
        cursor.execute(create_table_query)

    create_index_query = "CREATE INDEX IF NOT EXISTS idx_data ON test_table (data);"
    with connection.cursor() as cursor:
        cursor.execute(create_index_query)
    connection.commit()

    record_counts = [1000, 10000, 100000, 1000000]
    results = []

    for count in record_counts:
        print(f"\nTesting with {count} records...")

        insert_query = "INSERT INTO test_table (data) VALUES %s"
        data = [(f"Sample data {i}",) for i in range(count)]

        start_time = time.time()
        batch_insert(connection, insert_query, data, batch_size=10000)
        insert_time = time.time() - start_time

        select_query = "SELECT * FROM test_table"
        start_time = time.time()
        with connection.cursor() as cursor:
            cursor.execute(select_query)
            cursor.fetchall()
        select_time = time.time() - start_time

        update_query = "UPDATE test_table SET data = CONCAT(data, ' updated')"
        start_time = time.time()
        with connection.cursor() as cursor:
            cursor.execute(update_query)
        connection.commit()
        update_time = time.time() - start_time

        delete_query = "DELETE FROM test_table"
        start_time = time.time()
        with connection.cursor() as cursor:
            cursor.execute(delete_query)
        connection.commit()
        delete_time = time.time() - start_time

        results.append({
            "records": count,
            "insert_time": f"{insert_time:.2f} s",
            "select_time": f"{select_time:.2f} s",
            "update_time": f"{update_time:.2f} s",
            "delete_time": f"{delete_time:.2f} s"
        })

    print("\nPerformance Results:")
    for result in results:
        print(result)

finally:
    connection.close()
