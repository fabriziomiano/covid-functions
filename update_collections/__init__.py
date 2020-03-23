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
PCM_REGION_KEY = os.environ["PCM_REGION_KEY"]
try:
    DATE_KEY = os.environ["DATE_KEY"]
    PCM_DATE_FMT = os.environ["PCM_DATE_FMT"]
except KeyError:
    DATE_KEY = "data"
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
    # TODO: the following loops should obviously put into a function
    logging.info("Updating national collection...")
    for d in national_data:
        try:
            national_collection.update_one(
                {DATE_KEY: d[DATE_KEY]},
                {"$set": d},
                upsert=True
            )
        except Exception as e:
            logging.error(
                "Exception raised while processing {}: {}".format(d[DATE_KEY], e))
            logging.error("Type of parsed data: {}".format(type(d[DATE_KEY])))
    logging.info("Done with national data")
    logging.info("Updating regional collection...")
    for d in regional_data:
        try:
            regional_collection.update_one(
                {DATE_KEY: d[DATE_KEY], PCM_REGION_KEY: d[PCM_REGION_KEY]},
                {"$set": d},
                upsert=True
            )
        except Exception as e:
            logging.error(
                "Exception raised while processing {} {}: {}".format(
                    d[DATE_KEY], d[PCM_REGION_KEY], e))
            logging.error(
                "Type of parsed data: {} {}".format(type(d[DATE_KEY], type(d[PCM_REGION_KEY]))))
    logging.info("Done with regional data")
    logging.info("Updating provincial collection...")
    for d in provincial_data:
        try:
            provincial_collection.update_one(
                {DATE_KEY: d[DATE_KEY], PCM_PROVINCE_KEY: d[PCM_PROVINCE_KEY]},
                {"$set": d},
                upsert=True
            )
        except Exception as e:
            logging.error(
                "Exception raised while processing {} {}: {}".format(
                    d[DATE_KEY], d[PCM_PROVINCE_KEY], e))
            logging.error(
                "Type of parsed data: {} {}".format(type(d[DATE_KEY], type(d[PCM_PROVINCE_KEY]))))
    logging.info("Done with provincial data")
    logging.info(
        'Collections update finished at {}'.format(
            dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()
        )
    )
