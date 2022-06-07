import os
from datetime import date, timedelta
from modules.get_date import get_date
from modules.main_funcs import fetch_save_daily_data,create_view_table
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'
client = bigquery.Client()
datasets = client.list_datasets()
for dataset in datasets:
    tables = list(client.list_tables(dataset))
    for table in tables:
        print(client.get_table(table))


project_id = client.project
dest_dataset = "crimes_dataset"
view_dataset = "views"
delta = timedelta(days=1)
# columns = os.getenv("default_cols")
columns = ["unique_key", "case_number", "date",
            "block", "description", "location_description",
            "arrest", "location"]
os.environ["start_date"] = "2020, 01, 01"
os.environ["end_date"] = "2020, 03, 31"


def main(request):
    start_date = os.getenv("start_date")
    end_date = os.getenv("end_date")
    year, month, day = tuple(map(int, start_date.split(', ')))
    end_year, end_month, end_day = tuple(map(int, end_date.split(', ')))

    start_date = date(year, month, day)
    end_date = date(end_year, end_month, end_day)

    cur_date = get_date(start_date, end_date)
    raw_table_id = fetch_save_daily_data(cur_date, client, project_id, dest_dataset)
    if request["columns"] and isinstance(request["columns"], list or tuple):
        columns = ", ".join(request["columns"])
        create_view_table(client, raw_table_id, view_dataset, columns)
    return "success"

