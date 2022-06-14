import datetime
import json
import base64
import os
from datetime import datetime
from google.cloud import bigquery
from modules.CUD import table_insert_rows, update_values, delete_rows
from modules.utils import copy_table, table_exists

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'
SOURCE_TABLE_DATASET = os.getenv("source_dest")
DESTINATION_DATASET = os.getenv("dest_dataset")
# SOURCE_TABLE_DATASET = "bitcoindata-352508.crimes_dataset"
# DESTINATION_DATASET = "bitcoindata-352508.updated_crimes"
client = bigquery.Client()


def api_to_bigquery(event, context):
    now = datetime.now()

    current_time = now.strftime('%Y_%m_%d')

    if "data" in event:
        # decodes bytes data, replaces single quotes with double quotes
        # for json.loads() function
        data = base64. \
            b64decode(event["data"]) \
            .decode("utf-8") \
            .replace("'", '"')

        # converts string dict to dict type
        data = json.loads(data)

        request = data["request_type"]

        table_name = data["table_name"]
        dst_table_name = table_name + "_updated_" + current_time

        # source table/destination table name for copying tables
        source_table_id = SOURCE_TABLE_DATASET + "." + table_name
        dest_table = DESTINATION_DATASET + "." + dst_table_name

        # copy table from source to destination to update/add/delete
        if not table_exists(client, dest_table):
            copy_table(client, source_table_id, dest_table)

        # adds/updates/deletes rows/values according to request type
        if request == "post":
            rows_to_insert = data["rows"]
            table_insert_rows(client, dest_table, rows_to_insert)
        elif request == "put":
            column = data["column_name"]
            value = data["value"]
            update_values(client, dest_table, column, value)
        elif request == "delete":
            column = data["column_name"]
            value = data["value"]
            delete_rows(client, dest_table, column, value)
    else:
        print("no data in event")

    return "function finished successfully"
