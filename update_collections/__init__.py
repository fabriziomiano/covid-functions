import os
import datetime as dt
import logging
import requests
import azure.functions as func
from ..shared import client

NATIONAL_DATA_URL = os.environ["NATIONAL_DATA_URL"]
REGIONAL_DATA_URL = os.environ["REGIONAL_DATA_URL"]
PROVINCIAL_DATA_URL = os.environ["PROVINCIAL_DATA_URL"]
DB_NAME = os.environ["DB_NAME"]
PCM_PROVINCE_KEY = os.environ["PCM_PROVINCE_KEY"]
PCM_DATE_KEY = os.environ["PCM_DATE_KEY"]
try:
    PCM_DATE_FMT = os.environ["PCM_DATE_FMT"]
except KeyError:
    PCM_DATE_FMT = "%Y-%m-%d %H:%M:%S"

national_data = requests.get(NATIONAL_DATA_URL).json()
regional_data = requests.get(REGIONAL_DATA_URL).json()
provincial_data = requests.get(PROVINCIAL_DATA_URL).json()


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = dt.datetime.utcnow().replace(
        tzinfo=dt.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Running collections update timer-trigger function at {}'.format(utc_timestamp))
    national_collection = client[DB_NAME].national
    regional_collection = client[DB_NAME].regional
    provincial_collection = client[DB_NAME].provincial
    logging.info("Updating national collection")
    for d in national_data:
        date_dt = dt.datetime.strptime(d[PCM_DATE_KEY], PCM_DATE_FMT)
        d[PCM_DATE_KEY] = date_dt
        national_collection.update_one(
            {PCM_DATE_KEY: d[PCM_DATE_KEY]},
            {"$set": d},
            upsert=True
        )
    logging.info("Updating regional collection")
    for d in regional_data:
        date_dt = dt.datetime.strptime(d[PCM_DATE_KEY], PCM_DATE_FMT)
        d[PCM_DATE_KEY] = date_dt        
        regional_collection.update_one(
            {PCM_DATE_KEY: d[PCM_DATE_KEY]},
            {"$set": d},
            upsert=True
        )
    logging.info("Updating provincial collection")
    for d in provincial_data:
        date_dt = dt.datetime.strptime(d[PCM_DATE_KEY], PCM_DATE_FMT)
        d[PCM_DATE_KEY] = date_dt        
        provincial_collection.update_one(
            {PCM_DATE_KEY: d[PCM_DATE_KEY], PCM_PROVINCE_KEY: d[PCM_PROVINCE_KEY]},
            {"$set": d},
            upsert=True
        )
    logging.info(
        'Collections update finished at {}'.format(
            dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()
        )
    )
