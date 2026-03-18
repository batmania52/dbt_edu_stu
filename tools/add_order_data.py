import json
import psycopg2
import os

def get_db_info(db_conf):
    """
    Gets max ID from edu.order and customer ID range from edu.raw_customers.
    """
    conn = None
    cur = None
    max_order_id = 0
    min_customer_id = 1
    max_customer_id = 1
    try:
        conn = psycopg2.connect(
            host=db_conf['host'],
            port=db_conf['port'],
            user=db_conf['user'],
            password=db_conf['password'],
            database=db_conf['database']
        )
        cur = conn.cursor()

        # Get max order_id
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM edu.order;")
        max_order_id = cur.fetchone()[0]

        # Get customer ID range
        cur.execute("SELECT MIN(customer_id), MAX(customer_id) FROM edu.raw_customers;")
        result = cur.fetchone()
        if result and result[0] is not None:
            min_customer_id = result[0]
            max_customer_id = result[1]

        return max_order_id, min_customer_id, max_customer_id
    except Exception as e:
        print(f"Error getting DB info: {e}")
        return 0, 1, 1 # Default values in case of error
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def execute_sql_query(sql_query, db_conf, success_message="SQL query executed successfully."):
    """
    Executes a given SQL query against the PostgreSQL database.
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
        print(success_message)
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
            db_config['schema'] = 'edu'
    except FileNotFoundError:
        print(f"Error: dbconf.json not found at {dbconf_path}")
        exit(1)
    except KeyError:
        print(f"Error: 'postgres_remote' configuration not found in {dbconf_path}")
        exit(1)

    # Get necessary info from DB
    max_order_id, min_customer_id, max_customer_id = get_db_info(db_config)
    print(f"Current max order ID: {max_order_id}")
    print(f"Customer ID range: {min_customer_id} - {max_customer_id}")

    start_id = max_order_id + 1
    num_records_to_add = 10000

    insert_sql = f"""
    INSERT INTO edu.order (id, customer_id, order_date, total_amount)
    SELECT
        s.id AS id,
        FLOOR(RANDOM() * ({max_customer_id} - {min_customer_id} + 1) + {min_customer_id})::INTEGER AS customer_id,
        ('2025-01-01'::DATE + (FLOOR(RANDOM() * (DATE '2026-03-31' - DATE '2025-01-01' + 1)))::INTEGER)::DATE AS order_date,
        (RANDOM() * 1000 + 100)::NUMERIC(10, 2) AS total_amount
    FROM GENERATE_SERIES({start_id}, {start_id + num_records_to_add - 1}) AS s(id);
    """
    
    print(f"\nAdding {num_records_to_add} records to edu.order...")
    execute_sql_query(insert_sql, db_config, f"{num_records_to_add} records added to edu.order.")
