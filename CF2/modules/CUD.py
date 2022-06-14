import pandas as pd
import logging


def table_insert_rows(client, table_id, rows_to_insert):
    """
    function inserts row into table
    :param client: client object
    :param table_id: str
    :param rows_to_insert: dict

    """
    try:
        rows_df = pd.DataFrame([rows_to_insert])
        job = client.load_table_from_dataframe(rows_df, table_id)
        job.result()
        logging.info("New rows have been added.")
    except Exception as e:
        logging.error(f"Encountered errors while inserting rows: {e}")


def update_values(client, table_name, column, value):
    """
    function updates values in table
    :param client: client onject
    :param table_name: str
    :param column: str
    :param value: str
    """
    try:
        sql_all = f"""
                UPDATE {table_name} 
                SET {column} = '{value}' 
                WHERE 1=1;
                """

        query_job = client.query(sql_all)
        query_job.result()
        logging.info(f"column: {column} in table {table_name} updated")

    except Exception as e:
        message = f"Encountered errors while updating values: {e}"
        logging.error(message)


def delete_rows(client, table_name, column, value):
    """
    function deletes values in table
    :param client: client onject
    :param table_name: str
    :param column: str
    :param value: str
    """
    try:
        query_delete = f"""
            DELETE FROM {table_name} WHERE {column} = '{value}';
        """
        job = client.query(query_delete)
        job.result()

        logging.info( f"rows with value {value} deleted in {column}")

    except Exception as e:

        logging.error(f"Encountered errors while deleting values: {e}")
