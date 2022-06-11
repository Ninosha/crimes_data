from fastapi import FastAPI
from google.cloud import bigquery
import os

app = FastAPI()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/ninosha/Desktop/projects/achisproeqti/key.json"
client = bigquery.Client()


@app.get("/views")
def get_views():
    return {"Hello": "World"}


@app.post("/views")
def create_views():
    return "item"


@app.get("/data")
def read_data():
    return {"Hello": "World"}


@app.post("/data")
def insert_row():
    return "item"


@app.put("/data")
def update_row():
    return "item"


@app.delete("/data")
def delete_row():
    return "item"

# read data
# insert row/rows
# update values according to columns
#
# 2nd cloud function for crud
# fastapi cloud run for requests to cloud function

"""
crud viewebs vaketeb cloud runshi as app da roca rows insert update mpmodis pubsubit vugzavni dataflows da dataflow miketebs am chainsertebas da daaupdatebas
"""