import json
import requests
import os
from dotenv import load_dotenv

# Load variables from .env file into environment
load_dotenv()

# Dremio API Configurations
DREMIO_HOST = os.getenv("DREMIO_HOST")
DREMIO_API_TOKEN = os.getenv("DREMIO_API_TOKEN")
HEADERS = {"Authorization": f"Bearer {DREMIO_API_TOKEN}"}

COMPANY_ID = os.getenv("COMPANY_ID")
DB_PREFIX = '"saas-main-db".fintrip.'
VIEW_SPACE = os.getenv("VIEW_SPACE")



# Function to fetch all tables from saas-main-db.fintrip
def get_all_tables():
    """Fetch all tables from saas-main-db.fintrip"""
    response = requests.get(f"{DREMIO_HOST}/api/v3/catalog/by-path/saas-main-db/fintrip", headers=HEADERS)
    if response.status_code == 200:
        tables = [
            item["path"][-1]  # Extract table name from path
            for item in response.json().get("children", [])
            if item["type"] == "DATASET"
        ]
        return tables
    else:
        print(f"❌ Failed to fetch tables: {response.text}")
        return []

# Function to fetch all columns for a given table
def get_table_columns(table_name):
    """Fetch all available columns for a table using Dremio API"""
    response = requests.get(f"{DREMIO_HOST}/api/v3/catalog/by-path/saas-main-db/fintrip/{table_name}", headers=HEADERS)
    if response.status_code == 200:
        table_info = response.json()
        if "fields" in table_info:
            columns = [field["name"] for field in table_info["fields"]]
            return columns
        else:
            print(f"⚠️ No columns found for {table_name} in API response.")
            return []
    else:
        print(f"❌ Failed to fetch columns for {table_name}: {response.text}")
        return []
# Generate queries
queries = []
tables_columns = {}  # Dictionary to store table and columns info
all_tables = get_all_tables()

for table in all_tables:
    # Build source and target names
    source_table = f'{DB_PREFIX}"{table}"'
    target_view = f'{VIEW_SPACE}"{table}"'
    table_alias = table.replace(".", "_").replace("-", "_")

    # Get all columns from the table (even if the table is empty)
    all_columns = get_table_columns(table)
    all_columns = [col for col in all_columns if col not in ("date", "search", "month", "value")]
    tables_columns[table] = all_columns

    column_list = []

    # Alias all columns as "column_tablename"
    if not all_columns:
        # If for some reason no columns were fetched, create a dummy column.
        column_list.append("CAST(NULL AS VARCHAR) AS empty_table_placeholder")
    else:
        for column in all_columns:
            column_alias = f"{column}_{table_alias}"
            column_list.append(f'{column} AS {column_alias}')

    # Construct the SQL query for the view.
    query = f"CREATE OR REPLACE VDS {target_view} AS\nSELECT \n    " + ",\n    ".join(column_list)
    query += f"\nFROM {source_table} WHERE company_id = {COMPANY_ID};"

    if(table == "fintrip_programme_policies"):
        print(f"🔍 Query for {table}:\n{query}\n")

    # Log and store the query
    queries.append({"table": table, "query": query})

    # Execute the query via Dremio API to create/overwrite the view
    execute_query_payload = {"sql": query}
    execute_response = requests.post(f"{DREMIO_HOST}/api/v3/sql", headers=HEADERS, json=execute_query_payload)
    if execute_response.status_code == 200:
        print(f"🚀 View created for {table} successfully!")
    else:
        print(f"❌ Failed to create view for {table}: {execute_response.text}")

# Save queries to JSON file for reference
output_json_file = "generated_queries.json"
with open(output_json_file, "w", encoding="utf-8") as f:
    json.dump(queries, f, indent=4)

# Save table-column mappings to JSON file for reference
columns_json_file = "tables_columns.json"
with open(columns_json_file, "w", encoding="utf-8") as f:
    json.dump(tables_columns, f, indent=4)

print(f"✅ SQL queries saved as JSON in {output_json_file}")
print(f"✅ Table-Column mappings saved as JSON in {columns_json_file}")