from datetime import timedelta
import os

delta = timedelta(days=1)


def get_date(start, end):
    if start <= end:
        current_date = start.strftime("%Y-%m-%d")
        start += delta
        os.environ["start_date"] = str(start).replace("-", ", ")
        return current_date
