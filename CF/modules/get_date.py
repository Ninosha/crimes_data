from datetime import timedelta
import os
import logging

delta = timedelta(days=1)


def get_date(start, end):
    """
    function updates env variable which day has function to fetch
    :param start: str/from env variable
    :param end: sr/from env variable
    :return: str/current date
    """
    if start <= end:
        current_date = start.strftime("%Y-%m-%d")
        start += delta
        os.environ["start_date"] = str(start).replace("-", ", ")
        logging.info(f"start date is updated into")
        return current_date
