####################################################
# PACKAGE:  src.extract.transparent
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

import os
import sys
import time
import json
import toml
import atexit
import argparse
import threading
from typing import *
from math import ceil
from pathlib import Path
from datetime import date, datetime
from src.utils import *

Configs = Config()
Config(CACHE_PATH=Path('res/cache.db').resolve())
Config(CONF_PATH=Path('res/conf.toml').resolve())
Config(EARLIEST_DATE=datetime(year=2000, month=1, day=1))
Config(TIMESTAMP_FORMAT=DEFAULT_TIMESTAMP_FORMAT)
Config(KAFKA_TOPIC=None)
Config(DATE_FORMAT=None)
Config(USERDEFINED_START_DATE=None)
Config(USERDEFINED_END_DATE=None)
Config(TIMESTAMP=timestamp())

Resources = Resource()
Resource(KAFKA_CONSUMER=None)
Resource(KAFKA_PRODUCER=None)
Resource(SNOWFLAKE_CURSOR=None)
Resource(SNOWFLAKE_CONNECTION=None)
Resource(Cache=None)

def default_statistics_report():
    printlog("PING")

def start_reporter(statistics_report=default_statistics_report, heartbeat=300):
    def loop():
        while True:
            statistics_report()
            time.sleep(heartbeat)
    # Create a thread for the background task
    background_thread = threading.Thread(
        target=loop, name="Reporter")
    # Set the thread as a daemon so that it will exit when the main program ends
    background_thread.daemon = True
    # Start the background task
    background_thread.start()

def initialize_logger(logto: Path, prefix, timestamp, debugging, echo = True):
    logto = logto.resolve()
    logto.mkdir(parents=True, exist_ok=True)  # ensure path
    log_file_name = f"{logto}/{prefix}_{timestamp}.log"
    init_logger(log_file=log_file_name, debug=debugging)
    if echo:
        sys.stdout.write(log_file_name)

def destructor():
    printlog("Performing post-run cleanup", 'debug')
    if Resources.KAFKA_PRODUCER is not None:
        printlog("Flushing Kafka Producer...", 'debug')
        Resources.KAFKA_PRODUCER.flush()

    if Resources.Cache is not None:
        printlog("Commiting & Closing Cache Database...", 'debug')
        ApiEndpoint.update_quota()
        Resources.Cache.clean()

    if Resources.SNOWFLAKE_CURSOR is not None:
        printlog("Closing Snowflake Cursor...", 'debug')
        Resources.SNOWFLAKE_CURSOR.close()

    if Resources.SNOWFLAKE_CONNECTION is not None:
        printlog("Closing Snowflake Connection...", 'debug')
        Resources.SNOWFLAKE_CONNECTION.close()


def parse_arguements(title="Rapidapi Extraction Script"):
    parser = argparse.ArgumentParser(description=title)
    parser.add_argument(
        "--debug", '-d',
        action="store_true",
        help=MSG_ARG_DEBUG,
    )
    parser.add_argument(
        "--out", '-o',
        type=Path,
        default=Path("./logs").resolve(),
        help=MSG_ARG_LOGS_OUT,
    )
    parser.add_argument(
        "--schedule", '-sch',
        choices=["recurrent", "bootstrap", "custom"],
        type=str,
        required=True,
        help=MSG_ARG_SCHEDULE,
    )
    parser.add_argument(
        "--start", '-s',
        type=convert_to_timestamp,
        default=None,
        help=MSG_ARG_DATE_FORMAT
    )
    parser.add_argument(
        "--end", '-e',
        type=convert_to_timestamp,
        default=None,
        help=MSG_ARG_DATE_FORMAT
    )

    args = parser.parse_args()

    if args.schedule == "custom":
        if args.start is None and args.end is None:
            raise ValueError(
                "Custom schedule requires either --start, --end arguments.")

    Config(LOGTO=args.out)
    Config(DEBUG=args.debug)
    Config(SCHEDULE=args.schedule)
    Config(USERDEFINED_START_DATE=args.start)
    Config(USERDEFINED_END_DATE=args.end)
    return args


def define_schedule(
    schedule: str,
    DSD: datetime, # DEFAULT START DATE
    DED: datetime, # DEFAULT END DATE
    ED: datetime,  # EARLIEST DATE
    UDSD: datetime,# USER DEFINED START DATE
    UDED: datetime # USER DEFINED END DATE
) -> Tuple[datetime, datetime]:

    printlog("Defining Extraction Schedule", 'debug')
    # set schedule date/times
    if schedule == "recurrent":
        EXTRACT_START_DATE = DSD
        EXTRACT_END_DATE = DED
    elif schedule == "bootstrap":
        EXTRACT_START_DATE = ED
        EXTRACT_END_DATE = DED
    elif schedule == "custom":
        EXTRACT_START_DATE = coalesce(UDSD, DSD)
        EXTRACT_END_DATE = coalesce(UDED, DED)
    else:
        raise ValueError(f"Invalid schedule type: {schedule}")

    printlog("Schedule Metadata", "info", payload={
        "schedule": schedule,
        "start_date": EXTRACT_START_DATE,
        "end_date": EXTRACT_END_DATE,
    })

    Config(EXTRACT_START_DATE=EXTRACT_START_DATE)
    Config(EXTRACT_END_DATE=EXTRACT_END_DATE) 
    return EXTRACT_START_DATE, EXTRACT_END_DATE

def default_delivery_report(err, msg):
    """ 
    Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). 
    """
    if err is not None:
        printlog(f"REJECT", "error", {'reason': str(err), 'message': msg})
        # STATISTICS['Failure']['Load'] += 1
    # else:
    #     STATISTICS['Success']['Load'] += 1

@trace(message=MSG_CONST_LOAD_TO_KAFKA)
@handle(error_message=MSG_ERROR_SOURCE_LOAD_TO_KAFKA,
         strict=True,
         exit_signal=2,
         fallback_value=False)
def load_records_to_kafka(
        source_name: str,
        records: Generator[Dict, None, None],
        batch_key: str,
        kafka_broker: str,
        kafka_topic: str,
        chunk_size: int = 500,
        backoff_secs: int = 3,
        delivery_report: callable = default_delivery_report,
        source_metadata={}) -> bool:

    printlog(f"Kafka Metadata", "info", payload={'Kafka Metadata': {'topic_name':   kafka_topic,
                                                                    'batch_key':    batch_key,
                                                                    'chunk_size':   chunk_size}})
    index = 0
    with get_or_create_kafka_producer(kafka_broker) as producer:
        while True:
            load_task = Task(Type="load", Source=source_name, Metadata=source_metadata)
            record = None
            try:
                record = next(records)
                
                if not isinstance(record, Dict):
                    load_task = load_task.to_failed_task({'Bad Record': str(record)})
                    printlog("REJECT", "warning", load_task.__dict__)
                    continue
                
                # Serialize and send the Record
                b_record = default_value_serializer(record)
                b_key = default_key_serializer(batch_key)
                producer.produce(topic=kafka_topic,
                                 key=b_key,
                                 value=b_record,
                                 callback=delivery_report)
                index += 1
            except StopIteration as e1:
                # DONE Loading
                break
            except Exception as e2:
                load_task = load_task.to_failed_task({
                    'record': record,
                    'key': batch_key,
                    'index': index,
                    'reason': str(e2)
                })
                printlog("Record was not loaded", 'warning', load_task.__dict__)
                continue
            # wait to ensure all messages are sent
            if index % chunk_size == 0:
                printlog(f"Loaded Batch {index // chunk_size} to Kafka", "debug")
                producer.flush()
                time.sleep(backoff_secs)

    return True