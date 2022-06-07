from google.cloud import bigquery


def fetch_save_daily_data(cur_date, client, project_id,
                          dest_dataset):
    query_string = f"""
        SELECT * FROM
        `bigquery-public-data.chicago_crime.crime`
        WHERE
        date
        BETWEEN '{cur_date} 00:00:00' AND '{cur_date} 23:59:59'
        LIMIT 1000;
        """

    dataframe = (
        client.query(query_string)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    table_date = str(cur_date).replace("-", "_")
    table_id = f'{project_id}.{dest_dataset}.crimes_{table_date}'
    job = client.load_table_from_dataframe(
        dataframe, table_id
    )
    job.result()
    return table_id


def create_view_table(client, table_id, view_dataset):
    # project, dataset, table_name = table_id.split(".")
    view_table = table_id.replace(table_id.split(".")[1], view_dataset)
    # view_table_id = f"{project}.{view_dataset}.{table_name}"

    sql = f"""CREATE VIEW {view_table} AS
        SELECT 
        unique_key, case_number, date,
        block, description, location_description, 
        arrest, location	
        FROM {table_id}
        WHERE arrest = true;"""

    query_job = client.query(sql)
    query_job.result()
