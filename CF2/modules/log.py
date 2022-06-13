import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
from datetime import datetime
now = datetime.now()

current_time = now.strftime('%Y_%m_%d')
name = "crimes_2020_01_01"
print(name.replace(name[-10:],current_time))