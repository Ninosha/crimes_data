import os
from datetime import date, timedelta
from modules.get_date import get_date
from modules.main_funcs import fetch_save_daily_data, create_view_table
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
columns = os.getenv("default_cols")


def main(request):
    start_date = os.getenv("start_date")
    end_date = os.getenv("end_date")
    year, month, day = tuple(map(int, start_date.split(', ')))
    end_year, end_month, end_day = tuple(map(int, end_date.split(', ')))

    start_date = date(year, month, day)
    end_date = date(end_year, end_month, end_day)

    cur_date = get_date(start_date, end_date)
    raw_table_id = fetch_save_daily_data(cur_date, client, project_id, dest_dataset)
    create_view_table(client, raw_table_id, view_dataset, columns)
    return "success"

