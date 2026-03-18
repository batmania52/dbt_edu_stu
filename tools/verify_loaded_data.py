import json
import psycopg2
import csv
import os

def get_db_row_count(table_name, db_conf, schema_name='edu'):
    """
    Gets the row count of a table in the database.
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
        cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name};")
        return cur.fetchone()[0]
    except Exception as e:
        print(f"Error getting DB row count for {schema_name}.{table_name}: {e}")
        return -1
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_csv_row_count(file_path):
    """
    Gets the row count of a CSV file, excluding header.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)  # Skip header
            return sum(1 for row in reader)
    except FileNotFoundError:
        print(f"Error: CSV file not found at {file_path}")
        return -1
    except Exception as e:
        print(f"Error reading CSV file {file_path}: {e}")
        return -1

if __name__ == '__main__':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    dbconf_path = os.path.join(project_root, 'dbconf.json')
    input_data_dir = os.path.join(project_root, 'refs', 'datas')

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

    tables_to_verify = ['order_items', 'order', 'raw_products', 'raw_customers']

    print("\n--- Data Verification ---")
    for table_name in tables_to_verify:
        print(f"\nVerifying table: edu.{table_name}")
        
        db_count = get_db_row_count(table_name, db_config, db_config['schema'])
        csv_file_path = os.path.join(input_data_dir, f"{db_config['schema']}_{table_name}.csv")
        csv_count = get_csv_row_count(csv_file_path)

        print(f"  Rows in DB: {db_count}")
        print(f"  Rows in CSV: {csv_count}")

        if db_count == csv_count and db_count != -1:
            print("  Status: ✅ Counts match!")
        elif db_count == -1 or csv_count == -1:
            print("  Status: ⚠️ Error occurred during count, cannot compare.")
        else:
            print("  Status: ❌ Counts DO NOT match!")
