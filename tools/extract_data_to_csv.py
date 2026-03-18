import json
import psycopg2
import csv
import os

def extract_data_to_csv(table_name, output_dir, db_conf):
    """
    Extracts data from a specified PostgreSQL table and saves it as a CSV file.

    Args:
        table_name (str): The name of the table to extract data from.
        output_dir (str): The directory to save the CSV file.
        db_conf (dict): Database connection configuration.
    """
    try:
        conn = psycopg2.connect(
            host=db_conf['host'],
            port=db_conf['port'],
            user=db_conf['user'],
            password=db_conf['password'],
            database=db_conf['database']
        )
        cur = conn.cursor()

        schema_name = db_conf.get('schema', 'public') # Assuming 'edu' schema based on previous findings

        # Execute query to fetch data
        query = f"SELECT * FROM {schema_name}.{table_name};"
        cur.execute(query)
        rows = cur.fetchall()
        
        if not rows:
            print(f"No data found for table {schema_name}.{table_name}")
            return

        # Get column names
        column_names = [desc[0] for desc in cur.description]

        # Define output file path
        output_file = os.path.join(output_dir, f"{schema_name}_{table_name}.csv")
        
        # Write data to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(column_names)  # Write header
            csv_writer.writerows(rows)       # Write data rows
        
        print(f"Successfully extracted data from {schema_name}.{table_name} to {output_file}")

    except Exception as e:
        print(f"Error extracting data from {schema_name}.{table_name}: {e}")
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == '__main__':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    dbconf_path = os.path.join(project_root, 'dbconf.json')
    output_data_dir = os.path.join(project_root, 'refs', 'datas')
    
    # Ensure output directory exists
    os.makedirs(output_data_dir, exist_ok=True)

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

    tables_to_extract = ['order', 'order_items', 'raw_products', 'raw_customers']

    for table in tables_to_extract:
        extract_data_to_csv(table, output_data_dir, db_config)
