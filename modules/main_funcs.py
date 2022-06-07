from google.cloud import bigquery


def fetch_save_data(cur_date, client, project_id,
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
    table_id = f'{project_id}.{dest_dataset}.crimes-{cur_date}'
    job = client.load_table_from_dataframe(
        dataframe, table_id
    )
    job.result()
    return table_id


def create_view_table(client, cur_date, project_id,
                      dest_dataset, table_id):
    view_table_id = f"{project_id}.{dest_dataset}.view-crimes-{cur_date}"
    sql = f"""CREATE VIEW {view_table_id}
        OPTIONS(
            description = 'View table from crimes'
        ) AS
        SELECT unique_key
        FROM {table_id}
        WHERE arrest = true
        GROUP BY primary_type;"""
    query_job = client.query(sql)
    query_job.result()
