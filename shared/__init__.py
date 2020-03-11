import datetime as dt
import json
import logging
import os

import azure.functions as func
import pymongo
from bson import ObjectId

URI = os.environ["COSMOS_URI"]
try:
    DATE_FMT = os.environ["DATE_FMT"]
except KeyError:
    DATE_FMT = "%Y%m%d"
client = pymongo.MongoClient(URI)


def validate_request(req):
    """Return 405 if method isn't GET otherwise do nothing"""
    if req.method != "GET":
        return func.HttpResponse(status_code=405)
    else:
        pass


def is_good_date(date_str):
    """
    Return True if date string respects the agreed format else False
    :param date_str: str
    :return: bool
    """
    try:
        dt.datetime.strptime(date_str, DATE_FMT)
        return True
    except ValueError:
        return False


def validate_time_window(date_from, date_to):
    """
    Return True if date_from is in the past wrt date_to
    :param date_from:
    :param date_to:
    :return:
    """
    if not date_from or not date_to:
        return False
    else:
        return date_from <= date_to or False


def validate_dates(date_from, date_to):
    """
    Pass if dates are provided and with valid format, otherwise
    return False and error message
    :param date_from: str
    :param date_to: str
    :return: bool, str
    """
    if not date_from or not date_to:
        err_msg = "Please provide date_from and date_to in query string"
        logging.error(err_msg)
        return False, err_msg
    if not is_good_date(date_from) or not is_good_date(date_to):
        err_msg = "Invalid date provided"
        logging.error(err_msg)
        return False, err_msg
    if not validate_time_window(date_from, date_to):
        err_msg = "Wrong time interval: 'date from' > 'date to'"
        logging.error(err_msg)
        return False, err_msg
    return True, None


class JSONEncoder(json.JSONEncoder):
    """
    Custom encoder to deal with bson ObjectId and datetime.datetime objects
    """

    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, dt.datetime):
            return dt.datetime.strftime(o, DATE_FMT)
        return json.JSONEncoder.default(self, o)
