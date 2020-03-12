import datetime as dt
import logging
import os

import azure.functions as func

from ..shared import client, JSONEncoder, validate_request, validate_dates

DATE_KEY = os.environ["DATE_KEY"]
DB_NAME = os.environ["DB_NAME"]
PCM_DATE_KEY = os.environ["PCM_DATE_KEY"]
try:
    DATE_FMT = os.environ["DATE_FMT"]
except KeyError:
    DATE_FMT = "%Y%m%d"


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Return the data for a given time window"""
    logging.info("{} trigger function processed a request.".format(__name__))
    validate_request(req)
    date_from = req.params.get("from")
    date_to = req.params.get("to")
    validation_status, err_msg = validate_dates(date_from, date_to)
    if not validation_status:
        response = JSONEncoder().encode({"status": "KO", "error": err_msg})
        return func.HttpResponse(
            response, mimetype="application/json", status_code=400)
    date_from_dt = dt.datetime.strptime(date_from, DATE_FMT)
    date_to_dt = dt.datetime.strptime(date_to, DATE_FMT)
    query_timewindow = {DATE_KEY: {"$gte": date_from_dt, "$lte": date_to_dt}}
    national_collection = client[DB_NAME].national
    national_results = list(national_collection.find(query_timewindow))
    response = JSONEncoder().encode({
        "status": "OK",
        PCM_DATE_KEY: national_results
    })
    return func.HttpResponse(
        response, mimetype="application/json", status_code=200)
