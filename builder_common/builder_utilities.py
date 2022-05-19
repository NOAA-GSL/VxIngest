"""
Builder utilities - generic functions that builders use
"""

import datetime as dt
import logging

TS_OUT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def convert_to_iso(an_epoch):
    """
    simple conversion of an epoch to an iso string
    """
    if not isinstance(an_epoch, int):
        an_epoch = int(an_epoch)
    valid_time_str = dt.datetime.utcfromtimestamp(an_epoch).strftime(TS_OUT_FORMAT)
    return valid_time_str


def initialize_data(doc):
    """initialize the data by just making sure the template data element has been removed.
    All the data elements are going to be top level elements"""
    if "data" in doc.keys():
        del doc["data"]
    return doc


def get_geo_index(fcst_valid_epoch, geo):
    """return the geo index for the given fcst_valid_epoch

    Args:
        fcst_valid_epoch (int): an epoch in seconds
        geo (list): a list of VXingest geo objects

    Returns:
        int: the corresponding list index for the given geo and the given fcst_valid_epoch
    """
    latest_time = 0
    latest_index = 0
    try:
        geo_index = 0
        for geo_index, geo_item in enumerate(geo):
            if geo_item["lastTime"] > latest_time:
                latest_time = geo_item["lastTime"]
                latest_index = geo_index
            found = False
            if (
                geo_item["firstTime"] >= fcst_valid_epoch
                and fcst_valid_epoch <= geo_item["lastTime"]
            ):
                found = True
                break
        if found:
            return geo_index
        else:
            return latest_index
    except Exception as _e:  # pylint: disable=bare-except, disable=broad-except
        logging.error("CTCBuilder.get_geo_index: Exception  error: %s", str(_e))
        return 0


def truncate_round(_n, decimals=0):
    """
    Round a float to a specific number of places in an expected manner
    Args:
        n (int): the number of decimal places to use as a multiplier and divider
        decimals (int, optional): [description]. Defaults to 0.
    Returns:
        float: The number multiplied by n and then divided by n
    """
    multiplier = 10 ** decimals
    return int(_n * multiplier) / multiplier


def initialize_data_array(doc):
    """initialize the data by just making sure the template data element has been removed.
    All the data elements are going to be top level elements"""
    if "data" in doc.keys():
        del doc["data"]
    return doc
