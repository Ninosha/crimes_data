from google.cloud.exceptions import NotFound
import logging


def table_exists(client, dest_table_id):
    try:
        client.get_table(dest_table_id)
        return True
    except NotFound:
        return False


def copy_table(client, source_table_id, dest_table_id, bigquery):
    try:

        job = client.copy_table(source_table_id, dest_table_id)
        job.result()
        logging.info(f"{source_table_id} successfully copied in "
                     f"{dest_table_id}")

    except Exception as e:
        logging.error(
            f"Encountered errors while deleting values: {e}"
        )
