import yaml
import os

def generate_ddl_from_schema_yml(schema_file_path, output_ddl_dir, schema_prefix='edu'):
    """
    Generates CREATE TABLE DDLs for raw tables based on dbt staging models
    defined in a schema.yml file.

    Args:
        schema_file_path (str): Path to the schema.yml file.
        output_ddl_dir (str): Directory to save the generated DDL files.
        schema_prefix (str): The database schema name to use for the raw tables.
    """
    try:
        with open(schema_file_path, 'r', encoding='utf-8') as f:
            schema_data = yaml.safe_load(f)

        if not schema_data or 'models' not in schema_data:
            print(f"No models found in {schema_file_path}")
            return

        for model in schema_data['models']:
            model_name = model['name']
            
            # We are interested in generating DDL for the *source* tables that these stg models *would have referenced*
            # or in this case, for the stg models whose names directly map to the raw tables like edu_test001.sql
            # The schema.yml itself describes the *output* of the stg model.
            # We need DDL for the *input* tables.
            # However, the user's previous instruction was to infer DDL from schema.yml if DDL file doesn't exist.
            # So, for models like stg_test001, we generate DDL for edu_test001.

            # Determine the corresponding raw table name for which DDL is needed
            # Assuming stg_test001 -> edu_test001, stg_order -> edu_order
            if model_name.startswith('stg_'):
                raw_table_name_suffix = model_name[len('stg_'):]
                if raw_table_name_suffix in ['order', 'order_items']:
                    raw_table_name = raw_table_name_suffix
                else:
                    continue # Skip raw_products, raw_customers as they are handled manually
            else:
                continue # Only process stg_ models that map to raw source tables

            ddl_columns = []
            if 'columns' in model:
                for col in model['columns']:
                    col_name = col['name']
                    data_type = col['data_type']
                    # Simple mapping from dbt schema types to PostgreSQL types for DDL
                    # More complex mapping might be needed for production, but for training, this is sufficient.
                    if data_type == 'integer':
                        pg_type = 'INTEGER'
                    elif data_type == 'text' or data_type.startswith('varchar'):
                        pg_type = 'TEXT' # Simplified, can keep varchar(X) if char_len is present and needed
                        if data_type.startswith('varchar'):
                            pg_type = data_type.upper()
                    elif data_type.startswith('numeric'):
                        pg_type = data_type.upper()
                    elif data_type == 'date':
                        pg_type = 'DATE'
                    elif data_type.startswith('timestamp'):
                        pg_type = 'TIMESTAMP'
                    else:
                        pg_type = 'TEXT' # Default to TEXT if type is unknown or complex

                    ddl_columns.append(f"    {col_name} {pg_type}")
            
            if ddl_columns:
                ddl_content = f"CREATE TABLE IF NOT EXISTS {schema_prefix}.{raw_table_name} (\n"
                ddl_content += ",\n".join(ddl_columns)
                ddl_content += "\n);"

                output_file_path = os.path.join(output_ddl_dir, f"{schema_prefix}_{raw_table_name}.sql")
                with open(output_file_path, 'w', encoding='utf-8') as outfile:
                    outfile.write(ddl_content)
                print(f"Generated DDL for {schema_prefix}.{raw_table_name} to {output_file_path}")

    except FileNotFoundError:
        print(f"Error: schema.yml not found at {schema_file_path}")
    except yaml.YAMLError as e:
        print(f"Error parsing schema.yml: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    schema_yml_path = os.path.join(project_root, 'edu', 'models', 'stg', 'schema.yml')
    output_ddl_dir = os.path.join(project_root, 'refs', 'ddls')
    
    os.makedirs(output_ddl_dir, exist_ok=True) # Ensure DDL directory exists

    generate_ddl_from_schema_yml(schema_yml_path, output_ddl_dir)
