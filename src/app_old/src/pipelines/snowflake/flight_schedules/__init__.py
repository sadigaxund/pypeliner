####################################################
# PACKAGE:  src.extract.snowflake.flight_schedules
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

from src.pipelines.snowflake import *
from src.templates.Statistics import SNOWFLAKE_FLIGHT_SCHEDULES_STATISTICS as STATISTICS
import snowflake.connector
import decimal

Config(PAGE_SIZE=20)
Config(CODE_NAME='flight_schedules')
Config(REPORT_FREQUENCY=600)

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


@trace(message=MSG_INIT_SNOWFLAKE)
@handle(error_message=MSG_ERROR_INIT_SNOWFLAKE_CLIENT,
        strict=False,
        do_raise=True,
        timeout_secs=5,
        max_retries=5,
        exit_signal=2,)
def init_snowflake_client(
    _user: str,
    _password: str,
    _account: str
) -> None:
    Resources.SNOWFLAKE_CONNECTION = snowflake.connector.connect(
        user=_user,
        password=_password,
        account=_account,
    )
    Resources.SNOWFLAKE_CURSOR = Resources.SNOWFLAKE_CONNECTION.cursor()
    printlog("Snowflake connection established successfully.", 'debug')



if __name__ == "src.extract.snowflake.flight_schedules":
    atexit.register(destructor)
    constructor(script_title="OAG Flight Schedules",
                cache_table='airbnb',
                code_name=Configs.CODE_NAME)
    define_schedule(
        schedule=Configs.SCHEDULE,
        DSD=last_month(set_day=31),
        DED=today(midnight=True),
        ED=Configs.EARLIEST_DATE,
        UDSD=Configs.USERDEFINED_START_DATE,
        UDED=Configs.USERDEFINED_END_DATE,
    )
    start_reporter(statistics_report, heartbeat=Configs.REPORT_FREQUENCY)
    init_snowflake_client(
        Resources.USER, Resources.PASSWORD, Resources.ACCOUNT)
    printlog("Extractor Initialized", 'debug',
             {'Namespace': Configs.__dict__})
