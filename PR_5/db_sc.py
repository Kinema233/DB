import psycopg2
import time

DB_CONFIG = {
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
            cursor.executemany(query, batch)
    connection.commit()

def measure_query_time(connection, query, data=None):
    start_time = time.time()
    with connection.cursor() as cursor:
        if data:
            cursor.execute(query, data)
        else:
            cursor.execute(query)
    end_time = time.time()
    return end_time - start_time


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
    execute_query(connection, create_table_query)

    record_counts = [1000, 10000, 100000, 1000000]
    results = []

    for count in record_counts:
        print(f"\nTesting with {count} records...")

        insert_query = "INSERT INTO test_table (data) VALUES (%s);"
        data = [(f"Sample data {i}",) for i in range(count)]

        start_time = time.time()
        batch_insert(connection, insert_query, data, batch_size=10000)
        insert_time = time.time() - start_time

        select_query = "SELECT * FROM test_table LIMIT 1000;"
        select_time = measure_query_time(connection, select_query)

        update_query = "UPDATE test_table SET data = 'Updated data' WHERE id <= %s;"
        update_time = measure_query_time(connection, update_query, (min(1000, count),))

        delete_query = "DELETE FROM test_table WHERE id <= %s;"
        delete_time = measure_query_time(connection, delete_query, (min(1000, count),))

        results.append({
            "records": count,
            "insert_time": f"{insert_time:.2f} s",
            "select_time": f"{select_time:.2f} s",
            "update_time": f"{update_time:.2f} s",
            "delete_time": f"{delete_time:.2f} s"
        })

        execute_query(connection, "TRUNCATE TABLE test_table;")

    print("\nPerformance Results:")
    for result in results:
        print(result)

finally:
    connection.close()
