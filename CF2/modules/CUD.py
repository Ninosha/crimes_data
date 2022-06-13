import pandas as pd
import logging


def table_insert_rows(client, table_id, rows_to_insert):
    try:
        rows_df = pd.DataFrame([rows_to_insert])
        job = client.load_table_from_dataframe(rows_df, table_id)
        job.result()
        logging.info("New rows have been added.")
    except Exception as e:
        logging.error(
            f"Encountered errors while inserting rows: {e}"
        )


def update_values(client, table_name, column, value):
    try:
        sql_all = f"""
                UPDATE {table_name} 
                SET {column} = '{value}' 
                WHERE 1=1;
                """

        # columns = data.keys()
        # values = data.values()
        # update_statement = f'UPDATE {table_id} SET ({", ".join(columns)}) = ({", ".join(values)})'

        query_job = client.query(sql_all)
        query_job.result()
        logging.info(
            f"column: {column} in table {table_name} updated")
    except Exception as e:
        logging.error(
            f"Encountered errors while updating values: {e}"
        )


def delete_rows(client, table_name, column, value):
    try:
        query_delete = f"""
            DELETE FROM {table_name} WHERE {column} = '{value}';
        """
        job = client.query(query_delete)
        job.result()
        logging.info(f"Values {value} deleted in {column}")
    except Exception as e:
        logging.error(
            f"Encountered errors while deleting values: {e}"

        )

# def delete_table(table_id, client):
#     try:
#         client.delete_table(table_id)
#         logging.info(f"Deleted table {table_id}")
#     except Exception as e:
#         logging.error(
#             f"Encountered errors while deleting table: {e}"
#         )

