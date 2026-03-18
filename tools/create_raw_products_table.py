import json
import psycopg2
import os

def execute_sql_query(sql_query, db_conf):
    """
    Executes a given SQL query against the PostgreSQL database.

    Args:
        sql_query (str): The SQL query to execute.
        db_conf (dict): Database connection configuration.
    """
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=db_conf['host'],
            port=db_conf['port'],
            user=db_conf['user'],
            password=db_conf['password'],
            database=db_conf['database']
        )
        cur = conn.cursor()
        cur.execute(sql_query)
        conn.commit()
        print("SQL query executed successfully.")
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    dbconf_path = os.path.join(project_root, 'dbconf.json')

    try:
        with open(dbconf_path, 'r') as f:
            db_config = json.load(f)['postgres_remote']
            db_config['schema'] = 'edu' # Explicitly set schema to 'edu'
    except FileNotFoundError:
        print(f"Error: dbconf.json not found at {dbconf_path}")
        exit(1)
    except KeyError:
        print(f"Error: 'postgres_remote' configuration not found in {dbconf_path}")
        exit(1)

    create_raw_products_sql = """
    CREATE TABLE IF NOT EXISTS edu.raw_products AS
    SELECT
        id AS product_id
      , 'Product ' || id AS product_name
      , CASE (id % 3)
          WHEN 0 THEN 'Electronics'
          WHEN 1 THEN 'Books'
          ELSE 'Home Goods'
        END AS product_category
      , (random() * 100 + 1)::NUMERIC(10, 2) AS price
      , (CURRENT_DATE - (id % 365) * INTERVAL '1 day')::DATE AS created_date
    FROM (
      SELECT GENERATE_SERIES(1, 10000) AS id
    ) AS products
    WHERE 1=1
      AND (CURRENT_DATE - (id % 365) * INTERVAL '1 day')::DATE >= '2025-03-17'::DATE
      AND (CURRENT_DATE - (id % 365) * INTERVAL '1 day')::DATE < '2026-03-17'::DATE;
    """

    execute_sql_query(create_raw_products_sql, db_config)
