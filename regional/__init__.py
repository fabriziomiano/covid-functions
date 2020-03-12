import datetime as dt
import logging
import os
import re

import azure.functions as func

from ..shared import client, JSONEncoder, validate_request, validate_dates

DATE_KEY = os.environ["DATE_KEY"]
REGION_KEY = os.environ["REGION_KEY"]
DB_NAME = os.environ["DB_NAME"]
PCM_DATE_KEY = os.environ["PCM_DATE_KEY"]
try:
    DATE_FMT = os.environ["DATE_FMT"]
except KeyError:
    DATE_FMT = "%Y%m%d"


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Return the regional data for a given region in a given time window, if
    a region is specified, otherwise all the data in that time window
    """
    logging.info("{} trigger function processed a request.".format(__name__))
    validate_request(req)
    date_from = req.params.get("from")
    date_to = req.params.get("to")
    validation_status, err_msg = validate_dates(date_from, date_to)
    if not validation_status:
        response = JSONEncoder().encode({"status": "KO", "error": err_msg})
        return func.HttpResponse(
            response, mimetype="application/json", status_code=400)
    region = req.params.get("region")
    date_from_dt = dt.datetime.strptime(date_from, DATE_FMT)
    date_to_dt = dt.datetime.strptime(date_to, DATE_FMT)
    if not region:
        query_region = {DATE_KEY: {"$gte": date_from_dt, "$lte": date_to_dt}}
    else:
        query_region = {
            REGION_KEY: re.compile(region, re.IGNORECASE),
            PCM_DATE_KEY: {"$gte": date_from_dt, "$lte": date_to_dt}
        }
    regional_collection = client[DB_NAME].regional
    regional_results = list(regional_collection.find(query_region))
    response = JSONEncoder().encode({
        "status": "OK",
        PCM_DATE_KEY: regional_results
    })
    return func.HttpResponse(
        response, mimetype="application/json", status_code=200)
