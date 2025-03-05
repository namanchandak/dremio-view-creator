import json
import requests
import os
from dotenv import load_dotenv

load_dotenv()

VIEW_SPACE = os.getenv("VIEW_SPACE")


# Dremio API Configurations
DREMIO_HOST = os.getenv("DREMIO_HOST")
DREMIO_API_TOKEN = os.getenv("DREMIO_API_TOKEN")

HEADERS = {"Authorization": f"Bearer {DREMIO_API_TOKEN}"}

COMPANY_ID = os.getenv("COMPANY_ID")
DB_PREFIX = '"saas-main-db".fintrip.'

# -------------------------------------------------
# Load JSON from file
# -------------------------------------------------
json_file = os.getenv("JSON_FILE")
with open(json_file, "r", encoding="utf-8") as f:
    json_data = json.load(f)

# -------------------------------------------------
# Function to fetch all columns for a given table
# using the Dremio API
# -------------------------------------------------
def get_table_columns(table_name):
    """
    Fetch all available columns for a table using Dremio API.
    """
    url = f"{DREMIO_HOST}/api/v3/catalog/by-path/saas-main-db/fintrip/{table_name}"

    print(f"Fetching columns for {table_name} from Dremio API...")
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        table_info = response.json()
        if "fields" in table_info:
            return [field["name"] for field in table_info["fields"]]
        else:
            print(f"⚠️ No columns found for {table_name} in API response.")
            return []
    else:
        print(f"❌ Failed to fetch columns for {table_name}: {response.text}")
        return []

# -------------------------------------------------
# Generate queries
# -------------------------------------------------
queries = []     # Store queries as JSON array
sql_queries = [] # Store queries as SQL statements

# Iterate over tables in the JSON data
for full_table_name, columns in json_data.items():
    """
    Example of full_table_name: 'fintrip.fintrip_mapped_services'
    'columns' is a dict of { column_in_db: [json_keys, ...], ... } 
    like { "attrs": ["category"], "tags": ["entity", ...], ... }
    """

    # 1. Strip the leading 'fintrip.' if present
    clean_table_name = full_table_name.replace("fintrip.", "")

    # 2. Build the final FROM path for the SQL query
    from_table_path = f'{DB_PREFIX}"{clean_table_name}"'

    # 3. Fetch all columns for the table from Dremio
    all_columns = get_table_columns(clean_table_name)
    all_columns = [col for col in all_columns if col not in ("date", "search", "month", "value")]
    if not all_columns:
        print(f"⚠️ No columns fetched for table {clean_table_name}. Skipping.\n")
        continue

    # 4. Create aliases for each standard column while removing 'fintrip' and 'vc' prefixes
    column_list = []
    for col in all_columns:
        clean_alias = clean_table_name.replace("fintrip_", "").replace("vc_", "")  # Remove prefixes
        col_alias = f"{col}_{clean_alias}"
        column_list.append(f'{col} AS {col_alias}')

    # 5. Build REGEXP_EXTRACT columns based on JSON keys with the new aliasing format
    for json_col, keys in columns.items():
        json_col_alias = json_col.replace(".", "_").replace("-", "_")
        for key in keys:
            normalized_key = key.strip()
            key_alias = normalized_key.replace(".", "_").replace("-", "_")

            # Remove 'fintrip' and 'vc' from alias name
            clean_alias = clean_table_name.replace("fintrip_", "").replace("vc_", "")

            # New alias format: key_column_table
            final_alias = f"{key_alias}_{json_col}_{clean_alias}".replace(" ", "_").replace("@", "_").replace("*", "_").replace("/", "_")

            # print(f"Processing: {key_alias} -> {json_col} -> {clean_alias}")

            # Build query for handling both string and number (integer or decimal)
            column_list.append(
                f"COALESCE("
                f"NULLIF(REGEXP_EXTRACT({json_col}, '\"{normalized_key}\":\s*(\"[^\"]*\")', 1), ''), "  # Match string
                f"NULLIF(REGEXP_EXTRACT({json_col}, '\"{normalized_key}\":\s*(\d+(\.\d+)?)', 1), '')"   # Match number
                f") AS {final_alias}"
            )

    # 6. Construct the final SQL query
    query = "SELECT\n    " + ",\n    ".join(column_list)
    query += f"\nFROM {from_table_path} WHERE company_id = {COMPANY_ID};"

    vds_path = f'{VIEW_SPACE}"{clean_table_name}"'
    create_vds_sql = f'CREATE OR REPLACE VDS {vds_path} AS\n{query}'

    # 8) Execute the statement in Dremio
    payload = {"sql": create_vds_sql}
    response = requests.post(f"{DREMIO_HOST}/api/v3/sql", headers=HEADERS, json=payload)
    if response.status_code == 200:
        print(f"✅ Created/Updated VDS for table '{full_table_name}' successfully!")
    else:
        print(f"❌ Failed to create/update VDS for table '{full_table_name}': {response.text}")


    # 7. Store queries in both JSON and SQL structures
    queries.append({"table": full_table_name, "query": query})
    sql_queries.append(query)

# -------------------------------------------------
# Save queries as JSON array
# -------------------------------------------------
output_json_file = "generated_queries.json"
with open(output_json_file, "w", encoding="utf-8") as f:
    json.dump(queries, f, indent=4)

# -------------------------------------------------
# Save queries as SQL file
# -------------------------------------------------
output_sql_file = "generated_queries.sql"
with open(output_sql_file, "w", encoding="utf-8") as f:
    f.write("\n\n".join(sql_queries))

print(f"✅ SQL queries saved as JSON in `{output_json_file}`")
print(f"✅ SQL queries saved as SQL file in `{output_sql_file}`")