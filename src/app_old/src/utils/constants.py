
# Error Messages
MSG_ERROR_UNEXPECTED = "Unexpected Error Occurred"
MSG_ERROR_UNEXPECTED_INITIALIZING = "Unexpected Error Occurred while Initialization"
MSG_ERROR_UNEXPECTED_CONSTRUCTOR = "Unexpected Error Occurred at Constructor"
MSG_ERROR_UNEXPECTED_PIPELINE = "Unexpected Error Occurred while Executing Pipeline"

MSG_ERROR_SOURCE_LOAD_TO_KAFKA = "Possible Source Error while Loading to Kafka"
MSG_ERROR_SOURCE_EXTRACT = "Possible Source Error while Extracting"
MSG_ERROR_SOURCE_TRANSFORM = "Possible Source Error while Transforming"
MSG_ERROR_SOURCE_LOAD = "Possible Source Error while Loading"

MSG_ERROR_INIT_RAPIDAPI_CLIENT = "Error while initializing RapidAPI client"
MSG_ERROR_INIT_KAFKA_CLIENT = "Error while initializing Kafka client"
MSG_ERROR_INIT_SNOWFLAKE_CLIENT = "Failed to connect to Snowflake client"
MSG_ERROR_INIT_TRANSPARENT_CLIENT = "Error while initializing Metabase client"

# String Constants
MSG_CONST_PIPELINE = "Executing Pipeline: "
MSG_CONST_CONSTRUCTOR = "Executing Constructor"
MSG_CONST_LOAD_TO_KAFKA = "Loading Messages to Kafka"
MSG_CONST_EXTRACT_RECORDS = "Extracting Records from Source"
MSG_CONST_EXTRACT_REVIEWS = "Extracting Reviews from Source"
MSG_CONST_EXTRACT_LISTINGS = "Extracting Listings from Source"
MSG_CONST_EXTRACT_APIFY = "Extracting Results from Apify"



# Error Messages
MSG_ERROR_LOADING_ENV = "Error while loading .env file:"
MSG_ERROR_LOADING_ARGS = "Error while loading arguments:"
MSG_ERROR_LOADING_CONFIGS = "Error while loading configs:"
MSG_ERROR_LOADING_APIFY = "Error while connecting to Apify:"

MSG_ERROR_PARSE_ARGUMENTS = "Error while parsing arguments:"
MSG_ERROR_PARSE_PAYLOAD = "Error while parsing payload:"

MSG_ERROR_KAFKA_CREATE = "Error while creating Kafka topic:"
MSG_ERROR_KAFKA_LOAD = "Error while loading to Kafka:"
MSG_ERROR_KAFKA_PUBLISH = "Error while publishing a message to Kafka:"

MSG_ERROR_APIFY_LOAD = "Error while loading batch from Apify:"


MSG_ERROR_RECIEVE_REQUEST = "Error while receiving request:"
MSG_ERROR_HEALTH_CHECK = "Error while checking health:"
MSG_ERROR_TRANSFORM = "Error while transforming data:"
MSG_ERROR_DATE_FORMAT = "Error while formatting date:"
MSG_ERROR_DATE_FETCH = "Error while getting date:"

MSG_ERROR_EXTRACTING_REVIEWS = "Error while extracting reviews:"
MSG_ERROR_EXTRACTING_LISTINGS = "Error while extracting listings:"
MSG_ERROR_EXTRACTING_REGIONS = "Error while extracting regions:"



MSG_ERROR_BACKGROUND_DAEMON = "Unexpected Error with Background Daemon: " 

# Success messages
MSG_SUCCESSFUL_PIPELINE = "Pipeline Execution: Success!"
MSG_NOTSUCCESS_PIPELINE = "Pipeline Execution: Warning!"

# Initialization messages
MSG_INIT_RAPIDAPI = "Initializing Rapid API Client"
MSG_INIT_KAFKA = "Initializing Kafka Client"
MSG_INIT_SNOWFLAKE = "Initializing SnowSQL Client"
MSG_INIT_METABASE = "Initializing Metabase API Client"
MSG_INIT_QUERY = "Initializing Apify Query"


# Standard Template Messages
MSG_STM_ETL = "Executing ETL Process"
MSG_STM_EXT_REVIEWS = "Extracting Reviews from API"
MSG_STM_LOAD_ENV_VARS = "Loading Environment Variables"

# Command Line Argument Messages
MSG_ARG_DEBUG = "Whether to print to console or log to file"
MSG_ARG_SCHEDULE = "Schedule type. 'recurrent' for daily runs, 'bootstrap' for one-time run. Otherwise, 'custom' with either '--start' or '--end' parameters provided"
MSG_ARG_LOGS_OUT = "Directory to save the logs in (default: ./logs)."
MSG_ARG_DATE_FORMAT = "Format: 'YYYY-MM-DD HH:MM:SS'"
MSG_ARG_SOURCE = "Data source (available sources from source-configs.sqlite)"
MSG_ARG_HOST = "Host address to run the server on (default: localhost)"
MSG_ARG_PORT = "Port number to run the server on (default: 6060)"
# Custom Messages
__MSG_TEMPLATE_RAPIDAPI_CONNECTOR__ = "RapidAPI -> Kafka Connector: Extract & Load"
__MSG_TEMPLATE_SNOWFLAKE_CONNECTOR__ = "SnowflakeDB -> Kafka Connector: Extract & Load"
__MSG_TEMPLATE_METABASE_CONNECTOR__ = "Metabase API -> Kafka Connector: Extract & Load"
__MSG_TEMPLATE_APIFY_CONNECTOR__ = "Apify -> Kafka Connector:"

MSG_BOOKING_MAIN_REVIEWS = f"{__MSG_TEMPLATE_RAPIDAPI_CONNECTOR__} Reviews from Booking.com"
MSG_AIRBNB_MAIN_REVIEWS = f"{__MSG_TEMPLATE_RAPIDAPI_CONNECTOR__} Reviews from Airbnb.com"
MSG_HOTELS_MAIN_REVIEWS = f"{__MSG_TEMPLATE_RAPIDAPI_CONNECTOR__} Reviews from Hotels.com"
MSG_PRICELINE_MAIN_REVIEWS = f"{__MSG_TEMPLATE_RAPIDAPI_CONNECTOR__} Reviews from Priceline.com"
MSG_TRANSPARENT_MAIN_LISTINGS = f"{__MSG_TEMPLATE_METABASE_CONNECTOR__} Listings from Transparent's Database"
MSG_TRANSPARENT_MAIN_OPDATA = f"{__MSG_TEMPLATE_METABASE_CONNECTOR__} Operational data from Transparent's Database"
MSG_TRANSPARENT_MAIN_APIFY_QUERY_RUNNER = f"{__MSG_TEMPLATE_APIFY_CONNECTOR__} Start a Apify Query"
MSG_SNOWFLAKE_MAIN_SCHEDULES = "SnowflakeDB: Monthly Flight Schedules."
MSG_BOOKING_MAIN_LISTINGS = f"{__MSG_TEMPLATE_RAPIDAPI_CONNECTOR__} Listings from Booking.com"
MSG_AIRBNB_MAIN_LISTINGS = f"{__MSG_TEMPLATE_RAPIDAPI_CONNECTOR__} Listings from Airbnb.com"
MSG_HOTELS_MAIN_LISTINGS = f"{__MSG_TEMPLATE_RAPIDAPI_CONNECTOR__} Listings from Hotels.com"
MSG_PRICELINE_MAIN_LISTINGS = f"{__MSG_TEMPLATE_RAPIDAPI_CONNECTOR__} Listings from Priceline.com"

MSG_TRANSPARENT_AUTHENTICATION = "Authenticating to Transparent's Metabase Api"
MSG_ERROR_TRANSPARENT_AUTHENTICATION = "Error while Authenticating to Transparent's Metabase Api"
MSG_APIFY_CONNECT = "Connecting to Apify Client"


MSG_ARG_MAIN_PRICELINE_REVIEWS = "Kafka Connector that Extracts Reviews from Priceline.com & Loads to Kafka Topic"
MSG_ARG_MAIN_BOOKING_REVIEWS = "Kafka Connector that Extracts Reviews from Booking.com & Loads to Kafka Topic"
MSG_ARG_MAIN_HOTELS_REVIEWS = "Kafka Connector that Extracts Reviews from Hotels.com & Loads to Kafka Topic"
MSG_ARG_MAIN_AIRBNB_REVIEWS = "Kafka Connector that Extracts Reviews from Airbnb.com & Loads to Kafka Topic"
MSG_ARG_MAIN_TRANSPARENT_LISTINGS = "Kafka Connector that Extracts Listings from Transparent database & Loads to Kafka Topic"
MSG_ARG_MAIN_SNOWFLAKE_SCHEDULES = "Kafka Connector that Extracts Schedules from OAG's Snowflake database & Loads to Kafka Topic"

MSG_ARG_MAIN_PRICELINE_LISTINGS = "Kafka Connector that Extracts Listings from Priceline.com & Loads to Kafka Topic"
MSG_ARG_MAIN_BOOKING_LISTINGS = "Kafka Connector that Extracts Listings from Booking.com & Loads to Kafka Topic"
MSG_ARG_MAIN_HOTELS_LISTINGS = "Kafka Connector that Extracts Listings from Hotels.com & Loads to Kafka Topic"
MSG_ARG_MAIN_AIRBNB_LISTINGS = "Kafka Connector that Extracts Listings from Airbnb.com & Loads to Kafka Topic"
MSG_ARG_MAIN_APIFY_QUERY_RUNNER = "Start Apify query based on data source and type (aka. its frequency: recurrent or bootstrap)"
MSG_ARG_MAIN_APIFY_WEBHOOK_HANDLER = "Start Apify Webhook handler for loading finished queries from Apify"
