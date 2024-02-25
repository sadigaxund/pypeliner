####################################################
# PACKAGE:  src.extract.rapidapi.booking_listings
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
from src.templates.Statistics import BOOKING_LISTINGS_STATISTICS as STATISTICS

Config(PAGE_SIZE=500)
Config(CODE_NAME='booking_listings')
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
    merged = Resources.Cache.read('total') + Resources.Cache.read('current')  # merge
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
        STATISTICS['Extra']["Current Listings"]
    )
    printlog("STATISTICS", "trace", STATISTICS)


@trace(message=MSG_INIT_RAPIDAPI)
@handle(error_message=MSG_ERROR_INIT_RAPIDAPI_CLIENT,
        strict=False,
        do_raise=True,
        timeout_secs=5,
        max_retries=5)
def init_rapidapi_client(
    api_host: str,
    api_key: str,
) -> None:

    # Initialize the API Client
    HEADERS = {"X-RapidAPI-Key": api_key,
               "X-RapidAPI-Host": api_host}

    PROPERTIES_ENDPOINT = ApiEndpoint(
        name="Properties Endpoint",
        base_url=f"https://{api_host}",
        endpoint="/v1/static/hotels",
        parameters={
            "page": 0,
            "country": "sa"
        },
        headers=HEADERS,
    )
    
    METADATA_V1_ENDPOINT = ApiEndpoint(
        name="Metadata V1 Endpoint",
        base_url=f"https://{api_host}",
        endpoint="/v1/hotels/data",
        parameters={
            "hotel_id": "1377073",
            "locale": "en-us"
        },
        headers=HEADERS,
    )

    METADATA_V2_ENDPOINT = ApiEndpoint(
        name="Metadata V2 Endpoint",
        base_url=f"https://{api_host}",
        endpoint="/v2/hotels/details",
        parameters={
            "hotel_id": "1676161",
            "currency": "USD",
            "locale": "en-us",
            "checkin_date": date_to_string(today(), DEFAULT_TIMESTAMP_FORMAT),
            "checkout_date": date_to_string(next_month(), DEFAULT_TIMESTAMP_FORMAT)
        },
        headers=HEADERS,
    )

    Resource(PROPERTIES_ENDPOINT=PROPERTIES_ENDPOINT)
    Resource(METADATA_V1_ENDPOINT=METADATA_V1_ENDPOINT)
    Resource(METADATA_V2_ENDPOINT=METADATA_V2_ENDPOINT)

    # TEST
    test_endpoints(PROPERTIES_ENDPOINT, METADATA_V1_ENDPOINT,
                   METADATA_V2_ENDPOINT, debug=Configs.DEBUG)
    ApiEndpoint.set_quota(Resources.Cache, unlimited=True)


if __name__ == "src.extract.rapidapi.booking_listings":
    atexit.register(destructor)
    constructor(script_title="Booking Listings Extractor",
                cache_table='booking',
                code_name=Configs.CODE_NAME)
    start_reporter(statistics_report, heartbeat=Configs.REPORT_FREQUENCY)
    init_rapidapi_client(Configs.API_HOST, Resources.API_KEY)
    printlog("Extractor Initialized", 'debug',
             {'Namespace': Configs.__dict__})

