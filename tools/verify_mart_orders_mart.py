import json
import psycopg2
import os
from collections import OrderedDict

def execute_query_and_fetch_all(sql_query, db_conf):
    """
    Executes a SQL query and returns all rows with column names.
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
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        
        # Convert rows to list of OrderedDict for better comparison/display
        results = []
        for row in rows:
            results.append(OrderedDict(zip(columns, row)))
        return results

    except Exception as e:
        print(f"Error executing query: {e}")
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def print_data_comparison(source_data, target_data, source_name="Source", target_name="Target"):
    """
    Prints source and target data side-by-side for comparison.
    """
    print(f"\n--- {source_name} Data ---")
    if source_data:
        # Print headers
        print(", ".join(source_data[0].keys()))
        # Print rows
        for row in source_data:
            print(", ".join(map(str, row.values())))
    else:
        print("No data found.")

    print(f"\n--- {target_name} Data ---")
    if target_data:
        # Print headers
        print(", ".join(target_data[0].keys()))
        # Print rows
        for row in target_data:
            print(", ".join(map(str, row.values())))
    else:
        print("No data found.")

if __name__ == '__main__':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    dbconf_path = os.path.join(project_root, 'dbconf.json')

    try:
        with open(dbconf_path, 'r') as f:
            db_config = json.load(f)['postgres_remote']
            db_config['schema'] = 'edu' # Ensure the 'edu' database is connected
    except FileNotFoundError:
        print(f"Error: dbconf.json not found at {dbconf_path}")
        exit(1)
    except KeyError:
        print(f"Error: 'postgres_remote' configuration not found in {dbconf_path}")
        exit(1)

    # --- Verification for marts.orders_mart ---
    print("\n--- Verifying marts.orders_mart ---")

    # Date range for stg_order filtering
    start_date_var = "2025-01-01 00:00:00"
    end_date_var = "2026-04-01 00:00:00" # Exclusive end to cover up to 2026-03-31

    # 1. Get 5 sample order_ids from stg.stg_order that fall within the date range
    sample_order_ids_query = f"""
    SELECT order_id
    FROM stg.stg_order
    WHERE order_date >= '{start_date_var}'::timestamp
      AND order_date < '{end_date_var}'::timestamp
    ORDER BY order_id
    LIMIT 5;
    """
    sample_order_ids_results = execute_query_and_fetch_all(sample_order_ids_query, db_config)
    
    if not sample_order_ids_results:
        print("No sample order_ids found in stg.stg_order for the specified date range. Cannot proceed with verification.")
        exit(1)
        
    sampled_order_ids = tuple([row['order_id'] for row in sample_order_ids_results])
    print(f"Sampled Order IDs: {sampled_order_ids}")

    # 2. Extract source data (stg.stg_order JOIN stg.stg_order_items) for these order_ids and date range
    source_data_query = f"""
    SELECT
        o.order_id, o.customer_id, o.order_date, o.total_amount,
        oi.order_item_id, oi.product_id, oi.quantity, oi.price,
        (oi.quantity * oi.price) AS item_total_calculated
    FROM stg.stg_order o
    JOIN stg.stg_order_items oi ON o.order_id = oi.order_id
    WHERE o.order_id IN {sampled_order_ids}
      AND o.order_date >= '{start_date_var}'::timestamp
      AND o.order_date < '{end_date_var}'::timestamp
    ORDER BY o.order_id, oi.order_item_id;
    """
    source_data = execute_query_and_fetch_all(source_data_query, db_config)

    # 3. Extract target data (marts.orders_mart) for these order_ids
    target_data_query = f"""
    SELECT
        order_id, customer_id, order_date, total_amount,
        product_id, quantity, price, item_total
    FROM marts.orders_mart
    WHERE order_id IN {sampled_order_ids}
    ORDER BY order_id, product_id;
    """
    target_data = execute_query_and_fetch_all(target_data_query, db_config)

    # 4. Print comparison
    print_data_comparison(source_data, target_data, source_name="stg.stg_order JOIN stg.stg_order_items", target_name="marts.orders_mart")
