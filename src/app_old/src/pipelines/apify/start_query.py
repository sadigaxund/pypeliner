####################################################
# PACKAGE:  src.extract.apify
# ENTRY:    start_query.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# This script is creates predefined queries with recurrent variables
# and starts them asyncronously to run on Apify platform.
#
####################################################

from src.pipelines.apify import *

# < GLOBAL VARIABLES -----------------------------------------------------------

@trace(message=MSG_INIT_QUERY)
@handle(error_message=MSG_ERROR_LOADING_CONFIGS, 
        strict=False, 
        do_raise=True,
        max_retries=5, 
        timeout_secs=5, 
        exit_signal=1,
        on_error=statistics_report)
def init_apify_query(configs, apify_token):
    # Load Apify configurations from db
    # printlog(configs['kafka'])

    # Fetch Query Parameters
    Config(APIFY_ACTOR_ID=configs["id"])
    Config(APIFY_ACTOR_BUILD=configs.get("build", "latest"))
    Config(QUERY_LIMITERS=configs.get(f"{Configs.SCHEDULE}_limiters", "bootstrap_limiters"))
    Config(DATE_FORMAT=configs.get("date_format", ""))
    Config(DATE_LIMITER_KEY=configs.get("date_key", ""))
    Config(QUERY_INPUT=configs["input"])

    printlog(f"Loaded Actor ID: {Configs.APIFY_ACTOR_ID}", 'info')
    printlog(f"Loaded Actor Build: {Configs.APIFY_ACTOR_BUILD}", 'info')
    printlog(f"Loaded {Configs.SCHEDULE.capitalize()} Query Limiters: {Configs.QUERY_LIMITERS}", 'info')

    Config(RESULTS_LIMITER=Configs.QUERY_LIMITERS.pop("maxItems", None))
    Config(MEMORY_LIMITER=Configs.QUERY_LIMITERS.pop("memory", Configs.MAX_MEMORY_LIMIT))
    Config(TIME_LIMITER=Configs.QUERY_LIMITERS.pop("timeout", Configs.NO_TIMEOUT))

    if Configs.SCHEDULE == "recurrent":
        QUERY_INTERVAL = Configs.QUERY_LIMITERS.pop("interval", 1)
        DATE_LIMITER_VALUE = n_days_before(QUERY_INTERVAL, midnight=True)
    else:
        DATE_LIMITER_VALUE = Configs.EARLIEST_DATE

    printlog(f"Set Earliest Date: {DATE_LIMITER_VALUE}", 'info')
    printlog(f"Set Maximum Results: {Configs.RESULTS_LIMITER}", 'info')
    printlog(f"Set Maximum Memory (MBs): {Configs.MEMORY_LIMITER}", 'info')
    printlog(f"Set Timeout (Seconds): {Configs.NO_TIMEOUT}", 'info')

    if Configs.DATE_LIMITER_KEY != "":
        Configs.QUERY_INPUT[Configs.DATE_LIMITER_KEY] = date_to_string(
            DATE_LIMITER_VALUE, Configs.DATE_FORMAT)

    Configs.QUERY_INPUT.update(Configs.QUERY_LIMITERS)

    printlog(f"Generated Query Input: {Configs.QUERY_INPUT}", 'info')

    # Initialize the ApifyClient with your API token
    Resource(APIFY_CLIENT=ApifyClient(token=apify_token))
    # Fetch the actor object from the API
    Resource(APIFY_ACTOR=Resources.APIFY_CLIENT.actor(Configs.APIFY_ACTOR_ID))

    @trace(message=MSG_APIFY_CONNECT)
    @handle(error_message=MSG_ERROR_LOADING_APIFY, strict=False, max_retries=5, timeout_secs=5)
    def apify_connect():
        if Resources.APIFY_CLIENT.user().get() is None:
            raise ConnectionError("Unable to connect to Apify Client.")
    apify_connect()


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
        "--schedule", '-sch',
        choices=["recurrent", "bootstrap"],
        type=str,
        required=True,
        help=MSG_ARG_SCHEDULE,
    )
    parser.add_argument(
        "--source", '-src',
        type=str,
        required=True,
        help=MSG_ARG_SOURCE
    )

    args = parser.parse_args()

    # save arguments
    Config(LOGTO=args.out)
    Config(DEBUG=args.debug)
    Config(SCHEDULE=args.schedule)
    Config(SOURCE_NAME=args.source)
    return args


@handle(error_message=MSG_ERROR_UNEXPECTED_CONSTRUCTOR, 
        strict=True, 
        do_raise=True)
# NOTRACE: No logger is initialized yet.
def constructor() -> None:
    # Parse Command-Line Arguements
    parse_arguements()
    # Load environment variables
    with open(Configs.CONF_PATH, 'r') as f:
        confs = toml.loads(f.read())
        src_conf = confs['Sources']['Apify']
        Resource(TOKEN=src_conf['TOKEN'])  # For safety
        Config(MAX_RESULTS_LIMIT=src_conf['MAX_RESULTS_LIMIT'],
               MAX_MEMORY_LIMIT=src_conf['MAX_MEMORY_LIMIT'],
               NO_TIMEOUT=src_conf['NO_TIMEOUT'],
               EARLIEST_DATE=src_conf['EARLIEST_DATE'],
               KAFKA_BROKER=confs['Kafka']['BROKER'],
               KAFKA_TOPIC=confs['Kafka']['Topics'][Configs.SOURCE_NAME]['name'])
    initialize_logger(logto=Configs.LOGTO,
                      prefix="query_" + Configs.SOURCE_NAME,
                      timestamp=timestamp(),
                      debugging=Configs.DEBUG)
    
    # Load configurations from the database as
    # a JSON file based on the source name and initialize Kafka and Apify
    with open(Configs.SOURCE_CONFS.joinpath(f'{Configs.SOURCE_NAME}.toml'), 'r') as f:
        source_configs = toml.loads(f.read())
        init_apify_query(source_configs, Resources.TOKEN)
        
    printlog("Initialized Environment", 'debug', {
             'Namespace': str(Configs.__dict__)})


@handle(error_message=MSG_ERROR_UNEXPECTED_PIPELINE,
        strict=True,
        exit_signal=1,
        on_error=statistics_report)
# @notrace: no logger has been initialized at this point
def main():
    atexit.register(destructor)
    constructor()
    printlog("Source Configurations loaded & Query Input Generated!")
    # QUERY_STARTER = Resources.APIFY_ACTOR.call if Configs.SYNCED else Resources.APIFY_ACTOR.start

    run_info = Resources.APIFY_ACTOR.start(
        run_input=Configs.QUERY_INPUT,
        memory_mbytes=Configs.MEMORY_LIMITER,
        build=Configs.APIFY_ACTOR_BUILD,
        timeout_secs=Configs.TIME_LIMITER,
        max_items=Configs.RESULTS_LIMITER if Configs.RESULTS_LIMITER else None
    )

    printlog(f"Generated Run ID: {run_info['id']}", 'debug')
    printlog(f"Generated Dataset ID: {run_info['defaultDatasetId']}", 'debug')


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as e:
        print("Recieved Keyboard Interrupt. Stopping...")
