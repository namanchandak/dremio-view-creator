import json
import os
import subprocess

def run_query():
    """
    Function to execute the existing Python script that generates queries.
    """
    script_path = "/home/mohit/Projects/dice/Report-Analytics-Tool-BE/src/controller/dremio-script/create-fire-query.py"  # Replace with actual script filename

    try:
        result = subprocess.run(["python3", script_path], capture_output=True, text=True)
        if result.returncode == 0:
            return {"status": "success", "message": "Query executed successfully"}
        else:
            return {"status": "error", "message": result.stderr}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def read_json_file(filename):
    """
    Reads a JSON file and returns its contents.
    """
    if not os.path.exists(filename):
        return {"error": "File not found"}
    
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)

def read_sql_file(filename):
    """
    Reads a SQL file and returns its contents as a string.
    """
    if not os.path.exists(filename):
        return {"error": "File not found"}
    
    with open(filename, "r", encoding="utf-8") as f:
        return {"sql_queries": f.read()}
