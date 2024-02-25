####################################################
# PACKAGE:  src.extract.apify
# ENTRY:    __init__.py
####################################################


from src.pipelines import *
from src.templates.Statistics import APIFY_STATISTICS as STATISTICS
from apify_client import ApifyClient

Config(SOURCE_CONFS=Path('res/apify_sources').resolve())
Config(PAGE_SIZE=5000)
Config(REPORT_FREQUENCY=60)

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
        STATISTICS["Extra"]['Processed Pages'] * 100,
        STATISTICS["Extra"]["Dataset Pages"]
    )

    printlog("STATISTICS", "trace", STATISTICS)
