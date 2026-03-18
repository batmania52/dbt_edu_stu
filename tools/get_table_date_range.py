import json
import psycopg2
import os
import sys

def get_min_max_date_for_table(db_conf, table_name, date_column, schema_name='edu'):
    """
    Gets the min and max date for a specified date_column in a table.
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
        query = f"SELECT MIN({date_column}), MAX({date_column}) FROM {schema_name}.{table_name};"
        cur.execute(query)
        result = cur.fetchone()
        
        if result and result[0] is not None:
            # Ensure dates are formatted as 'YYYY-MM-DD HH:MM:SS' for dbt vars
            min_date = result[0].strftime('%Y-%m-%d %H:%M:%S') if hasattr(result[0], 'strftime') else str(result[0])
            max_date = result[1].strftime('%Y-%m-%d %H:%M:%S') if hasattr(result[1], 'strftime') else str(result[1])
            return min_date, max_date
        else:
            return None, None
    except Exception as e:
        print(f"Error getting min/max date for {schema_name}.{table_name}.{date_column}: {e}", file=sys.stderr)
        return None, None
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
        print(f"Error: dbconf.json not found at {dbconf_path}", file=sys.stderr)
        exit(1)
    except KeyError:
        print(f"Error: 'postgres_remote' configuration not found in {dbconf_path}", file=sys.stderr)
        exit(1)

    tables_to_check = [
        {'table_name': 'raw_products', 'date_column': 'created_date'},
        {'table_name': 'raw_customers', 'date_column': 'registration_date'}
    ]

    for item in tables_to_check:
        table_name = item['table_name']
        date_column = item['date_column']
        
        print(f"\n--- Date Range for edu.{table_name}.{date_column} ---")
        min_date, max_date = get_min_max_date_for_table(db_config, table_name, date_column, db_config['schema'])
        
        if min_date and max_date:
            # For dbt vars, end_date needs to be exclusive, so add 1 day to max_date
            # This requires converting back to datetime objects for calculation
            from datetime import datetime, timedelta
            max_date_dt = datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S')
            exclusive_end_date_dt = max_date_dt + timedelta(days=1)
            exclusive_end_date_str = exclusive_end_date_dt.strftime('%Y-%m-%d %H:%M:%S')

            print(f"  MIN({date_column}): {min_date}")
            print(f"  MAX({date_column}): {max_date}")
            print(f"  DBT_VARS start_date: '{min_date}'")
            print(f"  DBT_VARS end_date: '{exclusive_end_date_str}'")
        else:
            print("  No date range found or error occurred.")
