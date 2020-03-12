import os
import datetime
import logging
import requests
import azure.functions as func
from ..shared import client

NATIONAL_DATA_URL = os.environ["NATIONAL_DATA_URL"]
REGIONAL_DATA_URL = os.environ["REGIONAL_DATA_URL"]
PROVINCIAL_DATA_URL = os.environ["PROVINCIAL_DATA_URL"]
DB_NAME = os.environ["DB_NAME"]

national_data = requests.get(NATIONAL_DATA_URL).json()
regional_data = requests.get(REGIONAL_DATA_URL).json()
provincial_data = requests.get(PROVINCIAL_DATA_URL).json()


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Running collections update timer-trigger function at {}'.format(utc_timestamp))
    national_collection = client[DB_NAME].national
    regional_collection = client[DB_NAME].regional
    provincial_collection = client[DB_NAME].provincial
    logging.info("Updating national collection")
    for d in national_data:
        national_collection.update_one(
            {"data": d["data"]},
            {"$set": d},
            upsert=True
        )
    logging.info("Updating regional collection")
    for d in regional_data:
        regional_collection.update_one(
            {"data": d["data"]},
            {"$set": d},
            upsert=True
        )
    logging.info("Updating provincial collection")
    for d in provincial_data:
        provincial_collection.update_one(
            {"data": d["data"]},
            {"$set": d},
            upsert=True
        )
