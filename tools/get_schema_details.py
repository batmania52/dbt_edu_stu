import json
import psycopg2
import os

def get_table_schema(table_name, db_conf, schema_name='edu'):
    """
    Retrieves the schema (column names and types) for a given table.

    Args:
        table_name (str): The name of the table.
        db_conf (dict): Database connection configuration.
        schema_name (str): The schema name where the table resides.

    Returns:
        list: A list of dictionaries, each containing 'column_name' and 'data_type'.
    """
    conn = None
    cur = None
    schema_info = []
    try:
        conn = psycopg2.connect(
            host=db_conf['host'],
            port=db_conf['port'],
            user=db_conf['user'],
            password=db_conf['password'],
            database=db_conf['database']
        )
        cur = conn.cursor()

        query = f"""
        SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        cur.execute(query)
        rows = cur.fetchall()

        for row in rows:
            col_name, data_type, char_len, num_prec, num_scale = row
            type_str = data_type
            if data_type == 'character varying' and char_len is not None:
                type_str = f"varchar({char_len})"
            elif data_type == 'numeric' and num_prec is not None:
                if num_scale is not None and num_scale > 0:
                    type_str = f"numeric({num_prec}, {num_scale})"
                else:
                    type_str = f"numeric({num_prec})"
            
            schema_info.append({
                'column_name': col_name,
                'data_type': type_str
            })
        
        return schema_info

    except Exception as e:
        print(f"Error retrieving schema for {schema_name}.{table_name}: {e}")
        return []
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

    tables_to_inspect = ['raw_products', 'raw_customers'] # Inspecting raw tables as stg models directly select from them

    for table in tables_to_inspect:
        print(f"\n--- Schema for edu.{table} ---")
        schema = get_table_schema(table, db_config, db_config['schema'])
        for col in schema:
            print(f"- name: {col['column_name']}")
            print(f"  description: {col['column_name']}")
            print(f"  data_type: {col['data_type']}")
