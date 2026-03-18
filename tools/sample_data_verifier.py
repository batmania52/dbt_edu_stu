import json
import psycopg2
import os

def fetch_table_sample(table_name, db_conf, schema_name='stg', limit=5):
    """
    Fetches a sample of data from a specified table.
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
        query = f"SELECT * FROM {schema_name}.{table_name} LIMIT {limit};"
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        
        print(f"\n--- Sample data for {schema_name}.{table_name} (LIMIT {limit}) ---")
        if not rows:
            print("No data found.")
            return

        # Print header
        print(", ".join(columns))
        # Print rows
        for row in rows:
            print(", ".join(map(str, row)))

    except Exception as e:
        print(f"Error fetching sample data from {schema_name}.{table_name}: {e}")
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
    except FileNotFoundError:
        print(f"Error: dbconf.json not found at {dbconf_path}")
        exit(1)
    except KeyError:
        print(f"Error: 'postgres_remote' configuration not found in {dbconf_path}")
        exit(1)

    stg_tables = ['stg_order_items', 'stg_order', 'stg_product', 'stg_customer']

    for table_name in stg_tables:
        fetch_table_sample(table_name, db_config, schema_name='stg', limit=5)
