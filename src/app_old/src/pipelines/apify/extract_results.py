####################################################
# PACKAGE:  src.extract.apify
# ENTRY:    extract_results.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
# 
# DESCRIPTION:
# This script is used to extract finished results from Apify.
#
####################################################

from src.pipelines.apify import *


# < EXTRACT, TRANSFORM, LOAD -------------------------------------------------------------

@trace(MSG_CONST_EXTRACT_APIFY)
@handle(error_message=MSG_ERROR_SOURCE_EXTRACT, 
        strict=True, 
        exit_signal=2, 
        on_error=statistics_report)
def extract_from_apify(
    dataset_id: int,
    page_size: int = Configs.PAGE_SIZE
) -> Generator[Dict, None, None]:
    metadata = Resources.APIFY_CLIENT \
        .dataset(dataset_id) \
        .get()

    total_item_count = metadata['itemCount']
    clean_item_count = metadata['cleanItemCount']

    if total_item_count == 0:
        printlog(f"Dataset {dataset_id} has no items, skipping...", "warning")
        return

    pages = ceil(clean_item_count / page_size)

    STATISTICS['Extra']['Dataset ID'] = dataset_id
    STATISTICS['Extra']['Dataset Total'] = total_item_count
    STATISTICS['Extra']['Dataset Clean'] = clean_item_count
    STATISTICS['Extra']['Dataset Pages'] = pages
    STATISTICS['Extra']['Page Size'] = page_size

    printlog(f"Extracting Dataset: {dataset_id}", "info")
    printlog(f"Dataset Items Count: {total_item_count}", "info")
    printlog(f"Dataset Clean Items Count: {clean_item_count}", "info")
    printlog(f"Dataset Pages Count: {pages}", "info")
    printlog(f"Dataset Pages Size: {page_size}", "info")

    for page in range(0, pages):
        offset = page * page_size
        STATISTICS['Extra']['Last Processed Page'] = page
        STATISTICS['Extra']['Last Processed Offset'] = offset
        try:
            items = Resources.APIFY_CLIENT \
                .dataset(dataset_id) \
                .iterate_items(
                    offset=offset,
                    limit=page_size,
                    clean=True)

            # convert generator to list
            items_list = list(items)
            STATISTICS['Success']['Extract'] += len(items_list)
            STATISTICS['Extra']['Processed Pages'] += 1
            printlog(
                f"Extracted Clean Items=[{len(items_list)}] from Page=[{page + 1}/{pages}]", "debug")
            yield from items_list

        # in case of error, skip the page and continue to the next one
        except Exception as e:
            printlog(f"Failed to Extract Page=[{page + 1}/{pages}]", "warning")
            printlog(f"Failure Reason: {e}", "error")
            continue


# < MAIN FUNCTIONS -------------------------------------------------------------

def parse_arguements(title="Apify Query Script"):
    parser = argparse.ArgumentParser(description=title)
    parser.add_argument(
        "--out", '-o',
        type=Path,
        default="./logs",
        help=MSG_ARG_LOGS_OUT,
    )
    parser.add_argument(
        "--debug", '-d',
        action="store_true",
        help=MSG_ARG_DEBUG,
    )
    parser.add_argument(
        "--dataset", '-ds',
        required=True,
        type=str,
        help=MSG_ARG_PORT)
    parser.add_argument(
        "--source", '-src',
        required=True,
        type=str,
        help=MSG_ARG_HOST)

    args = parser.parse_args()

    # save arguments
    Config(LOGTO=args.out)
    Config(DEBUG=args.debug)
    Config(DATASET_ID=args.dataset)
    Config(SOURCE_NAME=args.source)
    return args

@trace(message=MSG_APIFY_CONNECT)
@handle(error_message=MSG_ERROR_LOADING_APIFY, 
        strict=False, 
        max_retries=5,
        timeout_secs=5,
        exit_signal=2,
        on_error=statistics_report)
def apify_connect():
    if Resources.APIFY_CLIENT.user().get() is None:
        raise ConnectionError("Unable to connect to Apify Client.")
        

@handle(error_message=MSG_ERROR_UNEXPECTED_CONSTRUCTOR, 
        strict=True, 
        do_raise=True)
# @notrace: no logger has been initialized at this point
def constructor() -> None:
    # Parse Command-Line Arguements
    parse_arguements()
    
    # Load environment variables
    with open(Configs.CONF_PATH, 'r') as f:
        confs = toml.loads(f.read())
        TOKEN = confs['Sources']['Apify']['TOKEN']
        Resource(APIFY_CLIENT=ApifyClient(token=TOKEN))  # For safety
        Config(KAFKA_BROKER=confs['Kafka']['BROKER'],
               KAFKA_TOPIC=confs['Kafka']['Topics'][Configs.SOURCE_NAME]['name'])
        
    initialize_logger(logto=Configs.LOGTO,
                      prefix="extract_" + Configs.SOURCE_NAME,
                      timestamp=timestamp(),
                      debugging=Configs.DEBUG)
    apify_connect()
    printlog("Extractor Initialized", 'debug',
             {'Namespace': Configs.__dict__})


@handle(error_message=MSG_ERROR_UNEXPECTED_PIPELINE,
        strict=True,
        exit_signal=1,
        on_error=statistics_report)
# @notrace: no logger has been initialized at this point
def main():
    atexit.register(destructor)
    constructor()
    start_reporter(statistics_report, heartbeat=Configs.REPORT_FREQUENCY)
    
    # load all the items to Kafka
    if load_records_to_kafka(records=extract_from_apify(dataset_id=Configs.DATASET_ID),
                             batch_key=timestamp(),
                             source_name=Configs.SOURCE_NAME,
                             kafka_topic=Configs.KAFKA_TOPIC,
                             kafka_broker=Configs.KAFKA_BROKER,
                             delivery_report=message_delivery_report,):
        printlog(MSG_SUCCESSFUL_PIPELINE, "info")
        statistics_report()  # Report one last time
    else:
        printlog(MSG_SUCCESSFUL_PIPELINE, "info")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as e:
        printlog("Recieved Keyboard Interrupt. Stopping...", 'warning')
