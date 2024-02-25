####################################################
# PACKAGE:  src.extract.rapidapi.airbnb_reviews
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
from src.templates.Statistics import AIRBNB_REVIEWS_STATISTICS as STATISTICS

Config(PAGE_SIZE=20)
Config(CODE_NAME='airbnb_reviews')
Config(REPORT_FREQUENCY=3)

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

    REVIEWS_ENDPOINT=ApiEndpoint(
        name="Reviews Endpoint",
        base_url=f"https://{api_host}",
        endpoint="/v2/listingReviews",
        parameters={  # Sort order: ASC
            "id": "619966061834034729",  # Example
            "date_time": "2023-01-11 10:52:50"  # Example
        },
        headers=HEADERS,
    )

    CONNECTIVITY_ENDPOINT=ApiEndpoint(
        name="Connectivity Endpoint",
        base_url=f"https://{api_host}",
        endpoint="/v2",
        headers=HEADERS
    )

    Resource(REVIEWS_ENDPOINT=REVIEWS_ENDPOINT)
    
    # TEST
    test_endpoints(CONNECTIVITY_ENDPOINT, REVIEWS_ENDPOINT, debug=Configs.DEBUG)
    ApiEndpoint.set_quota(Resources.Cache, unlimited=Configs.SCHEDULE == 'bootstrap')


if __name__ == "src.extract.rapidapi.airbnb_reviews":
    atexit.register(destructor)
    constructor(script_title="Airbnb Reviews Extractor",
                cache_table='airbnb',
                code_name=Configs.CODE_NAME)
    define_schedule(
        schedule=Configs.SCHEDULE,
        DSD=yesterday(midnight=True),
        DED=today(midnight=True),
        ED=Configs.EARLIEST_DATE,
        UDSD=Configs.USERDEFINED_START_DATE,
        UDED=Configs.USERDEFINED_END_DATE,
    )
    start_reporter(statistics_report, Configs.REPORT_FREQUENCY)
    init_rapidapi_client(Configs.API_HOST, Resources.API_KEY)
    printlog("Extractor Initialized", 'debug',
             {'Namespace': Configs.__dict__})

