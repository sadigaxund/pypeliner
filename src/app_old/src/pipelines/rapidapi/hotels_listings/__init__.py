####################################################
# PACKAGE:  src.extract.rapidapi.hotels_listings
# ENTRY:    __init__.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# ...
#
####################################################

from src.pipelines.rapidapi import *
from src.templates.Statistics import HOTELS_LISTINGS_STATISTICS as STATISTICS

Config(PAGE_SIZE=500)
Config(CODE_NAME='hotels_listings')
Config(REPORT_FREQUENCY=300)

Resource(PREVIOUS_LISTINGS=[])
Resource(CURRENT_LISTINGS=[])
Resource(TOTAL_LISTINGS=[])

def recache_listings():
    """
    Update Cache:
    1. Push current to previous                 | for keeping track of changes
    2. Populate current with the new listings   | saving ID of current run to monitor
    3. Merge save everything unique to total    | for keeping track of all listings
    """
    Resources.PREVIOUS_LISTINGS = Resources.Cache.read("current")
    Resources.Cache.write("previous", Resources.PREVIOUS_LISTINGS)  # 1
    Resources.Cache.write("current", Resources.CURRENT_LISTINGS)  # 2
    merged = Resources.Cache.read(
        'total') + Resources.Cache.read('current')  # merge
    total = list(set(merged))
    Resources.Cache.write("total", total)  # 3
    printlog("Done Caching Listings", 'info', {
        "No. of Previous Listings": len(Resources.PREVIOUS_LISTINGS),
        "No. of Current Listings": len(Resources.CURRENT_LISTINGS),
        "No. of All Known Listings": len(total),
    })


def message_delivery_report(cachier, err, msg):
    """ 
    Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). 
    """
    if err is not None:
        printlog(f"REJECT", "error", {'reason': str(err), 'message': msg})
        STATISTICS['Failure']['Load'] += 1
    else:
        STATISTICS['Success']['Load'] += 1
        STATISTICS['Extra']['Size'] += len(msg.value())
        record = default_value_deserializer(msg.value())
        Resources.CURRENT_LISTINGS.append(cachier(record))  # add to cache


def statistics_report() -> None:
    """ 
    Called once per heartbeat of reporter. Heartbeat defined by 'REPORT_FREQUENCY' configuration.
    """
    STATISTICS['Extra']['Completion'] = safe_divide(
        STATISTICS['Success']["Load"] * 100,
        STATISTICS['Extra']["Current Listings"])

    printlog("STATISTICS", "trace", STATISTICS)


@trace(message=MSG_INIT_RAPIDAPI)
@handle(error_message=MSG_ERROR_INIT_RAPIDAPI_CLIENT,
        strict=False,
        do_raise=True,
        timeout_secs=5,
        max_retries=5,
        on_error=statistics_report)
def init_rapidapi_client(
    api_host: str,
    api_key: str,
) -> None:

    # Initialize the API Client
    HEADERS = {"X-RapidAPI-Key": api_key,
               "X-RapidAPI-Host": api_host}

    REGIONS_ENDPOINT = ApiEndpoint(
        name="Regions Endpoint",
        base_url=f"https://{api_host}",
        endpoint="/v2/regions",
        parameters={
            "domain": "US",
            "query": "Saudi Arabia",
            "locale": "en_US"
        },
        headers=HEADERS
    )

    LISTINGS_ENDPOINT = ApiEndpoint(
        name="Listings Endpoint",
        base_url=f"https://{api_host}",
        endpoint="/v2/hotels/search",
        parameters={
            "sort_order": "DISTANCE",
            "locale": "en_US",
            "checkin_date": timestamp(),
            "checkout_date": date_to_string(next_month(), DEFAULT_TIMESTAMP_FORMAT),
            "adults_number": "1",
            "domain": "US",
            "region_id": '178043',
            "page_number": 1
        },
        headers=HEADERS,
    )

    INFO_ENDPOINT = ApiEndpoint(
        name="Hotel Info Endpoint",
        base_url=f"https://{api_host}",
        endpoint="/v2/hotels/info",
        parameters={
            "hotel_id": "19795040",
            "locale": "en_US",
            "domain": "US"
        },
        headers=HEADERS,
    )
    DETAILS_ENDPOINT = ApiEndpoint(
        name="Hotel Details Endpoint",
        base_url=f"https://{api_host}",
        endpoint="/v2/hotels/details",
        parameters={
            "hotel_id": "19795040",
            "locale": "en_US",
            "domain": "US"
        },
        headers=HEADERS,
    )
    
    Resource(REGIONS_ENDPOINT=REGIONS_ENDPOINT)
    Resource(LISTINGS_ENDPOINT=LISTINGS_ENDPOINT)
    Resource(INFO_ENDPOINT=INFO_ENDPOINT)
    Resource(DETAILS_ENDPOINT=DETAILS_ENDPOINT)

    # TEST
    test_endpoints(REGIONS_ENDPOINT, LISTINGS_ENDPOINT,
                   INFO_ENDPOINT, DETAILS_ENDPOINT, debug=Configs.DEBUG)
    ApiEndpoint.set_quota(Resources.Cache, unlimited=True)


if __name__ == "src.extract.rapidapi.hotels_listings":
    atexit.register(destructor)
    constructor(script_title="Hotels Listings Extractor",
                cache_table='hotels',
                code_name=Configs.CODE_NAME)
    start_reporter(statistics_report, heartbeat=Configs.REPORT_FREQUENCY)
    init_rapidapi_client(Configs.API_HOST, Resources.API_KEY)
    printlog("Extractor Initialized", 'debug',
             {'Namespace': Configs.__dict__})
