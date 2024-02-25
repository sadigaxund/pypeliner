####################################################
# PACKAGE:  src.extract.rapidapi.booking_reviews
# ENTRY:    __init__.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# This script extracts details of booking listings from Rapidapi data provider.
#
####################################################

from src.pipelines.rapidapi import *
from src.templates.Statistics import BOOKING_REVIEWS_STATISTICS as STATISTICS

Config(PAGE_SIZE=25)
Config(CODE_NAME='booking_reviews')
Config(REPORT_FREQUENCY=150)


def message_delivery_report(err, msg):
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


def statistics_report() -> None:
    """ 
    Called once per heartbeat of reporter. Heartbeat defined by 'REPORT_FREQUENCY' configuration.
    """
    STATISTICS["Extra"]['Completion'] = safe_divide(
        STATISTICS["Extra"]['Processed Listings'] * 100,
        STATISTICS["Extra"]["Available Listings"]
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

    REVIEWS_ENDPOINT = ApiEndpoint(
        name="Reviews Endpoint",
        base_url=f"https://{api_host}",
        endpoint="/v1/hotels/reviews",
        parameters={  # Sort order: DESC
            "sort_type": "SORT_RECENT_DESC",
            "hotel_id": "1676161",
            "locale": "en-us"
        },
        headers=HEADERS
    )

    Resource(REVIEWS_ENDPOINT=REVIEWS_ENDPOINT)

    # TEST
    test_endpoints(REVIEWS_ENDPOINT, debug=Configs.DEBUG)
    ApiEndpoint.set_quota(Resources.Cache, unlimited=True)


if __name__ == "src.extract.rapidapi.booking_reviews":
    atexit.register(destructor)
    constructor(script_title="Booking Reviews Extractor",
                cache_table='booking',
                code_name=Configs.CODE_NAME)
    define_schedule(
        schedule=Configs.SCHEDULE,
        DSD=yesterday(midnight=True),
        DED=today(midnight=True),
        ED=Configs.EARLIEST_DATE,
        UDSD=Configs.USERDEFINED_START_DATE,
        UDED=Configs.USERDEFINED_END_DATE,
    )
    start_reporter(statistics_report, heartbeat=Configs.REPORT_FREQUENCY)
    init_rapidapi_client(Configs.API_HOST, Resources.API_KEY)
    printlog("Extractor Initialized", 'debug',
             {'Namespace': Configs.__dict__})
