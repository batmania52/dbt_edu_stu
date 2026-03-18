import json
import psycopg2
import os

def get_order_date_range_for_initial_records(db_conf):
    """
    Gets the min and max order_date for initial 50 records in edu.order.
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
        cur.execute("SELECT MIN(order_date), MAX(order_date) FROM edu.order WHERE id <= 50;")
        return cur.fetchone()
    except Exception as e:
        print(f"Error getting order_date range: {e}")
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
        print(f"Error: dbconf.json not found at {dbconf_path}")
        exit(1)
    except KeyError:
        print(f"Error: 'postgres_remote' configuration not found in {dbconf_path}")
        exit(1)

    min_date, max_date = get_order_date_range_for_initial_records(db_config)
    print(f"Original 50 records order_date range: MIN={min_date}, MAX={max_date}")
