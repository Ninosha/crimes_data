from google.cloud.exceptions import NotFound
import logging


def table_exists(client, dest_table_id):
    """

    :param client:
    :param dest_table_id:
    :return:
    """
    try:
        client.get_table(dest_table_id)
        logging.info(f"{dest_table_id} exists")
        return True
    except NotFound:
        logging.info(f"{dest_table_id} doesn't exist")
        return False


def copy_table(client, source_table_id, dest_table_id):
    """
    function copies table from one dataset to updated dataset
    :param client: client object
    :param source_table_id: str
    :param dest_table_id: str
    """
    try:

        job = client.copy_table(source_table_id, dest_table_id)
        job.result()
        logging.info(f"{source_table_id} successfully copied in "
                     f"{dest_table_id}")

    except Exception as e:
        logging.error(
            f"Encountered errors while deleting values: {e}"
        )
