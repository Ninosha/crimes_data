from fastapi import FastAPI
from google.cloud import bigquery
import os
from modules.utils import message, create_push
from modules.read import read
from vars import TOPIC_ID, TABLE_DATASET, VIEWS_DATASET

app = FastAPI()
os.environ[
    'GOOGLE_APPLICATION_CREDENTIALS'] = "/home/ninosha/Desktop/projects/achisproeqti/key.json"
client = bigquery.Client()
project_id = client.project


@app.get("/data")
def read_data(table_name: str):
    data = read(client, TABLE_DATASET, table_name)
    return {"data": data}


@app.post("/data")
def insert_rows(table_name: str, rows: dict or list):
    req_type = "post"
    data = message({"request_type": req_type, "table_name": table_name,
                    "rows": rows})
    create_push(project_id, TOPIC_ID, data)
    return {"client_host": [table_name, rows]}


@app.put("/data")
def update_row(table_name: str, column_name: str, value: str or int):
    req_type = "put"
    data = message({"request_type": req_type, "table_name": table_name,
                    "column_name": column_name,
                    "value": value})
    create_push(project_id, TOPIC_ID, data)
    return "success"


@app.delete("/data")
def delete_row(table_name: str, column_name: str, value: str or int):
    req_type = "delete"
    data = message({"request_type": req_type, "table_name": table_name,
                    "column_name": column_name,
                    "value": value})
    create_push(project_id, TOPIC_ID, data)
    return "item"


@app.get("/views")
def read_views(table_name: str):
    data = read(client, VIEWS_DATASET, table_name)
    return {"data": data}

# @app.delete("/table")
# def delete_table(table_name: str):
#     req_type = "delete"
#     data = message({"request_type": req_type, "table_name": table_name,
#                     "column_name": column_name,
#                     "value": value})
#     create_push(project_id, topic_id, data)
#     return "item"
