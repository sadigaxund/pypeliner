####################################################
# PACKAGE:  src.extract.transparent.airbnb_operational
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

from src.pipelines.transparent import *
from src.templates.Statistics import TRANSPARENT_OPERATIONAL_STATISTICS as STATISTICS
from urllib.parse import quote_plus

Config(CODE_NAME='airbnb_operational')
Config(REPORT_FREQUENCY=10)


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
    STATISTICS['Extra']['Completion'] = safe_divide(
        STATISTICS['Success']['Load'] * 100,
        STATISTICS['Success']['Extract']
    )
    printlog("STATISTICS", "trace", STATISTICS)


if __name__ == "src.extract.transparent.airbnb_operational":
    atexit.register(destructor)
    constructor(script_title="Airbnb Operational Data Extractor",
                cache_table='airbnb',
                code_name=Configs.CODE_NAME)
    define_schedule(
        schedule=Configs.SCHEDULE,
        DSD=n_days_before(days=2, midnight=True),
        DED=today(midnight=True),
        ED=Configs.EARLIEST_DATE,
        UDSD=Configs.USERDEFINED_START_DATE,
        UDED=Configs.USERDEFINED_END_DATE,
    )
    start_reporter(statistics_report, heartbeat=Configs.REPORT_FREQUENCY)
    init_transparent_client(api_host=Configs.API_HOST,
                            api_key=Resources.PASSWORD,
                            api_user=Resources.USERNAME)
    printlog("Extractor Initialized", 'debug',
             {'Namespace': Configs.__dict__})
