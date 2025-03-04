import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os
import json
import time
import sys

load_dotenv()

DB_HOST     = os.getenv("DB_HOST")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DATABASE    = os.getenv("DATABASE")
company_id  = sys.argv[1] if len(sys.argv)>1 else os.getenv("COMPANY_ID")

def log(message):
    """Write log messages to stderr."""
    sys.stderr.write(message + "\n")

def connect_to_db():
    """Establishes a MySQL connection, automatically reconnecting if needed."""
    retries = 3
    for attempt in range(retries):
        try:
            connection = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DATABASE
            )
            if connection.is_connected():
                log(f"✅ Connected to MySQL database: {DATABASE}")
                return connection
        except Error as e:
            log(f"⚠️ MySQL Connection Failed (Attempt {attempt+1}/{retries}): {e}")
            time.sleep(3)
    log("❌ Could not establish connection to MySQL. Exiting.")
    exit()

def get_table_columns_map(connection):
    """
    Returns a dictionary mapping full table names (schema.table) to a list of column names 
    that have DATA_TYPE 'text'.
    """
    table_columns_map = {}
    with connection.cursor() as cursor:
        query = """
        SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND DATA_TYPE IN ('text');
        """
        cursor.execute(query, (DATABASE,))
        results = cursor.fetchall()
    log(f"🔍 Found {len(results)} tables containing JSON/TEXT columns in schema '{DATABASE}':")
    for schema_name, table_name, column_name, data_type in results:
        full_table_name = f"{schema_name}.{table_name}"
        table_columns_map.setdefault(full_table_name, []).append(column_name)
    return table_columns_map

def extract_keys(json_obj, prefix=""):
    """
    Recursively extract all unique keys from nested JSON objects and arrays.
    For dictionary values, the full key is built by concatenating parent keys with a dot.
    Returns a set of key strings.
    """
    keys = set()
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            full_key = f"{prefix}.{key}" if prefix else key  
            keys.add(full_key)
            keys.update(extract_keys(value, full_key))
    elif isinstance(json_obj, list):
        for item in json_obj:
            keys.update(extract_keys(item, prefix))
    return keys

def process1():
    """
    Process 1: Extract all JSON keys from text columns.
    Returns a dictionary mapping each table/column to a list of keys.
    """
    table_keys_map = {}
    connection = connect_to_db()
    try:
        with connection.cursor() as cursor:
            query = """
            SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND DATA_TYPE IN ('text');
            """
            cursor.execute(query, (DATABASE,))
            results = cursor.fetchall()
        
        log(f"🔍 [Process 1] Found {len(results)} tables with JSON/TEXT columns in schema '{DATABASE}':")
        table_columns_map = {}
        for schema_name, table_name, column_name, data_type in results:
            full_table_name = f"{str(schema_name)}.{str(table_name)}"
            table_columns_map.setdefault(full_table_name, []).append(str(column_name))
        
        for full_table_name, present_columns in table_columns_map.items():
            schema_name, table_name = full_table_name.split(".")
            log(f"⚡ [Process 1] Processing `{schema_name}`.`{table_name}` (Columns: {', '.join(present_columns)})")
            
            for column_name in present_columns:
                json_query = f"""
                SELECT `{column_name}`
                FROM `{schema_name}`.`{table_name}`
                WHERE `{column_name}` IS NOT NULL
                  AND company_id = {company_id};
                """
                try:
                    if not connection.is_connected():
                        log("🔄 [Process 1] Reconnecting to MySQL...")
                        connection = connect_to_db()
                    with connection.cursor() as cursor:
                        cursor.execute(json_query)
                        rows = cursor.fetchall()
                    
                    extracted_keys = set()
                    for row in rows:
                        json_data = row[0]
                        if json_data and isinstance(json_data, str):
                            json_data = json_data.strip()
                            if json_data.startswith("{") or json_data.startswith("["):
                                try:
                                    parsed_json = json.loads(json_data)
                                    extracted_keys.update(extract_keys(parsed_json))
                                except json.JSONDecodeError as json_error:
                                    log(f"⚠️ [Process 1] JSON Decoding Error in `{schema_name}`.`{table_name}`: {json_error}")
                    
                    if extracted_keys:
                        table_keys_map.setdefault(full_table_name, {})[column_name] = list(extracted_keys)
                        log(f"✅ [Process 1] Extracted {len(extracted_keys)} keys from `{schema_name}`.`{table_name}` (Column: `{column_name}`)")
                except Error as table_error:
                    log(f"❌ [Process 1] Error fetching data from `{schema_name}`.`{table_name}` ({column_name}): {table_error}")
    finally:
        if connection.is_connected():
            connection.close()
            log("🔒 [Process 1] MySQL connection closed.")
    
    # Optionally write to file
    output_file = "extracted_json_keys.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(table_keys_map, f, indent=4)
    log(f"📄 [Process 1] Wrote extracted keys to {output_file}")
    return table_keys_map

def extract_nested_keys(json_obj):
    """
    Recursively extract 'id' values from JSON objects in a structured format.
    Returns a list of unique 'id' values.
    """
    extracted_ids = set()
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            if isinstance(value, (dict, list)):
                extracted_ids.update(extract_nested_keys(value))
    elif isinstance(json_obj, list):
        for item in json_obj:
            if isinstance(item, dict) and all(k in item for k in ["key", "id", "value"]):
                extracted_ids.add(item["id"])
            elif isinstance(item, (dict, list)):
                extracted_ids.update(extract_nested_keys(item))
    return list(extracted_ids)

def process2():
    """
    Process 2: Extract nested keys from JSON arrays.
    Returns a dictionary mapping each table to its nested keys.
    """
    table_data_map = {}
    connection = connect_to_db()
    try:
        with connection.cursor() as cursor:
            query = """
            SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND DATA_TYPE IN ('text');
            """
            cursor.execute(query, (DATABASE,))
            results = cursor.fetchall()
        
        log(f"🔍 [Process 2] Found {len(results)} tables with JSON/TEXT columns in schema '{DATABASE}':")
        table_columns_map = {}
        for schema_name, table_name, column_name, data_type in results:
            full_table_name = f"{str(schema_name)}.{str(table_name)}"
            table_columns_map.setdefault(full_table_name, []).append(str(column_name))
        
        for full_table_name, present_columns in table_columns_map.items():
            schema_name, table_name = full_table_name.split(".")
            log(f"⚡ [Process 2] Processing `{schema_name}`.`{table_name}` (Columns: {', '.join(present_columns)})")
            
            agg_nested_keys = set()
            for column_name in present_columns:
                json_query = f"""
                SELECT `{column_name}`
                FROM `{schema_name}`.`{table_name}`
                WHERE `{column_name}` IS NOT NULL
                  AND company_id = {company_id};
                """
                try:
                    if not connection.is_connected():
                        log("🔄 [Process 2] Reconnecting to MySQL...")
                        connection = connect_to_db()
                    with connection.cursor() as cursor:
                        cursor.execute(json_query)
                        rows = cursor.fetchall()
                    
                    for row in rows:
                        json_data = row[0]
                        if json_data and isinstance(json_data, str):
                            json_data = json_data.strip()
                            if json_data.startswith("{") or json_data.startswith("["):
                                try:
                                    parsed_json = json.loads(json_data)
                                    nested_keys = extract_nested_keys(parsed_json)
                                    agg_nested_keys.update(nested_keys)
                                except json.JSONDecodeError as err:
                                    log(f"⚠️ [Process 2] JSON Decoding Error in `{schema_name}`.`{table_name}` ({column_name}): {err}")
                except Error as table_error:
                    log(f"❌ [Process 2] Error fetching data from `{schema_name}`.`{table_name}` ({column_name}): {table_error}")
            
            if agg_nested_keys:
                table_data_map[full_table_name] = {
                    "forms": {
                        "nested_keys": sorted(list(agg_nested_keys))
                    }
                }
                log(f"✅ [Process 2] {full_table_name}: Collected {len(agg_nested_keys)} nested keys.")
    finally:
        if connection.is_connected():
            connection.close()
            log("🔒 [Process 2] MySQL connection closed.")
    
    # Optionally write to file
    output_file = "extracted_json_array_data.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(table_data_map, f, indent=4)
    log(f"📄 [Process 2] Wrote nested keys data to {output_file}")
    return table_data_map

def main():
    # Run both processes and combine their outputs
    keys_output = process1()
    nested_output = process2()
    final_output = {
        "json_keys": keys_output,
        "json_array_data": nested_output
    }
    # Output the final dictionary as pure JSON to stdout
    print(json.dumps(final_output))

if __name__ == "__main__":
    main()
