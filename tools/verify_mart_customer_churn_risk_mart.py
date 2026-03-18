import json
import psycopg2
import os
from collections import OrderedDict
from datetime import datetime, timedelta

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
        print(", ".join(source_data[0].keys()))
        for row in source_data:
            print(", ".join(map(str, row.values())))
    else:
        print("No data found.")

    print(f"\n--- {target_name} Data ---")
    if target_data:
        print(", ".join(target_data[0].keys()))
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
            db_config['schema'] = 'edu'
    except FileNotFoundError:
        print(f"Error: dbconf.json not found at {dbconf_path}")
        exit(1)
    except KeyError:
        print(f"Error: 'postgres_remote' configuration not found in {dbconf_path}")
        exit(1)

    # --- Verification for marts.customer_churn_risk_mart ---
    print("\n--- Verifying marts.customer_churn_risk_mart ---")

    # The analysis_date is fixed to the last dbt run's run_started_at
    # For consistency, use '2026-03-16' as it was when the dbt run was successful
    analysis_date_var = '2026-03-16' 
    
    # 1. Get 5 sample customer_ids from stg.stg_customer
    sample_customer_ids_query = f"""
    SELECT customer_id
    FROM stg.stg_customer
    ORDER BY customer_id
    LIMIT 5;
    """
    sample_customer_ids_results = execute_query_and_fetch_all(sample_customer_ids_query, db_config)
    
    if not sample_customer_ids_results:
        print("No sample customer_ids found in stg.stg_customer. Cannot proceed with verification.")
        exit(1)
        
    sampled_customer_ids = tuple([row['customer_id'] for row in sample_customer_ids_results])
    print(f"Sampled Customer IDs: {sampled_customer_ids}")

    # 2. Extract source data (stg.stg_customer JOIN stg.stg_order aggregation) for these customer_ids
    # This query needs to replicate the logic inside customer_churn_risk_mart up to the final SELECT
    source_data_query = f"""
    with customer_orders as (
        select
              customer_id
            , max(order_date) as last_order_date
            , count(order_id) as total_orders
            , avg(total_amount) as avg_order_value
        from stg.stg_order
        where customer_id IN {sampled_customer_ids}
        group by 1
    ),
    customers as (
        select
              customer_id
            , customer_name
            , customer_email
            , registration_date
        from stg.stg_customer
        where customer_id IN {sampled_customer_ids}
    )
    select
          c.customer_id
        , c.customer_name
        , c.customer_email
        , c.registration_date
        , co.last_order_date
        , (DATE '{analysis_date_var}' - co.last_order_date::date) as days_since_last_order
        , co.total_orders
        , co.avg_order_value
        , ( (DATE '{analysis_date_var}' - co.last_order_date::date) * 0.5 - co.total_orders * 0.1 )::numeric(10, 2) as churn_risk_score
        , case
            when ( (DATE '{analysis_date_var}' - co.last_order_date::date) * 0.5 - co.total_orders * 0.1 ) > 50 then 'High'
            when ( (DATE '{analysis_date_var}' - co.last_order_date::date) * 0.5 - co.total_orders * 0.1 ) > 20 then 'Medium'
            else 'Low'
          end as churn_risk_segment
        , '{analysis_date_var}'::date as analysis_date
    from customers c
    left join customer_orders co
      on c.customer_id = co.customer_id
    where co.customer_id is not null
    ORDER BY c.customer_id;
    """
    source_data = execute_query_and_fetch_all(source_data_query, db_config)

    # 3. Extract target data (marts.customer_churn_risk_mart) for these customer_ids
    target_data_query = f"""
    SELECT
        customer_id, customer_name, customer_email, registration_date,
        last_order_date, days_since_last_order, total_orders, avg_order_value,
        churn_risk_score, churn_risk_segment, analysis_date
    FROM marts.customer_churn_risk_mart
    WHERE customer_id IN {sampled_customer_ids}
      AND analysis_date = '{analysis_date_var}'::date
    ORDER BY customer_id;
    """
    target_data = execute_query_and_fetch_all(target_data_query, db_config)

    # 4. Print comparison
    print_data_comparison(source_data, target_data, source_name="stg.stg_customer JOIN stg.stg_order Aggregation", target_name="marts.customer_churn_risk_mart")
