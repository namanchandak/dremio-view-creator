from fastapi import FastAPI
import query_runner

app = FastAPI()

# 1️⃣ API to run the query generation script
@app.get("/run-query")
async def run_query():
    return query_runner.run_query()

# 2️⃣ API to return JSON data with generated queries
@app.get("/get-query-result")
async def get_query_result():
    return query_runner.read_json_file("generated_queries.json")

# 3️⃣ API to return SQL file content
@app.get("/get-queries-sql")
async def get_queries_sql():
    return query_runner.read_sql_file("generated_queries.sql")
