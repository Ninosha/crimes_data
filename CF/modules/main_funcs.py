import logging


def fetch_save_daily_data(cur_date, client, project_id,
                          dest_dataset):
    """
    function fetches daily data for 3 months/saves to bgq table
    :param cur_date: str
    :param client: client obj
    :param project_id: str
    :param dest_dataset: str
    :return: str/saved table_id
    """
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
    logging.info(f" data from day {cur_date} fetched/saved")
    return table_id


def create_view_table(client, table_id, view_dataset, columns):
    """
    function creates table view in gbq in dataset: views
    :param client: client obj
    :param table_id: str
    :param view_dataset: str from env
    :param columns: str list from env
    :return:
    """
    view_table = table_id.replace(table_id.split(".")[1], view_dataset)

    sql = f"""CREATE VIEW {view_table} AS
        SELECT 
        {columns}
        FROM {table_id}; """

    query_job = client.query(sql)
    query_job.result()
    logging.info(f"view table for {table_id} created")

