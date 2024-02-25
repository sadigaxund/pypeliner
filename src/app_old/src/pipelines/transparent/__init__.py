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

from src.pipelines import *


@trace(message=MSG_TRANSPARENT_AUTHENTICATION)
@handle(error_message=MSG_ERROR_TRANSPARENT_AUTHENTICATION,
        strict=False,
        do_raise=True,
        timeout_secs=5,
        max_retries=5)
def authenticate(uname: str,
                 password: str,
                 host: str) -> Union[Exception, Dict]:
    AUTH_ENDPOINT = ApiEndpoint(
        base_url=f"https://{host}",
        endpoint="/api/session",
        headers={"Content-Type": "application/json"},
        data={
            "username": uname,
            "password": password
        },
    )
    response = AUTH_ENDPOINT.POST()
    # Check the response
    if response.status_code == 200:
        return response.json()
    else:
        raise PermissionError(
            f"Request failed with status code {response.status_code}: {response.text}")


@trace(message=MSG_INIT_METABASE)
@handle(error_message=MSG_ERROR_INIT_RAPIDAPI_CLIENT,
        strict=False,
        do_raise=True,
        timeout_secs=5,
        max_retries=5)
def init_transparent_client(
    api_user: str,
    api_key: str,
    api_host: str,
):

    auth_key = authenticate(api_user, api_key, api_host)
    HEADERS = {'Content-Type': 'application/json',
               'X-Metabase-Session': auth_key['id']}

    DEMAND_ENDPOINT = ApiEndpoint(
        name="Transparent Operational Endpoint",
        headers=HEADERS
    )
    SUPPLY_ENDPOINT = ApiEndpoint(
        name="Transparent Listings Endpoint",
        headers=HEADERS
    )
    
    CONNECTIVITY_ENDPOINT = ApiEndpoint(
        name="Transparent Metabase API",
        base_url=f'https://{api_host}',
        endpoint='/',
    )
    Resource(DEMAND_ENDPOINT=DEMAND_ENDPOINT)
    Resource(SUPPLY_ENDPOINT=SUPPLY_ENDPOINT)
    Resource(CONNECTIVITY_ENDPOINT=CONNECTIVITY_ENDPOINT)

    test_endpoints(Resources.CONNECTIVITY_ENDPOINT)

@handle(error_message=MSG_ERROR_UNEXPECTED_CONSTRUCTOR, 
        strict=True, 
        do_raise=True)
# NOTRACE: No logger is initialized yet.
def constructor(script_title, cache_table, code_name) -> None:
    # Parse Command-Line Arguements
    parse_arguements(script_title)
    
    # Load environment variables
    with open(Configs.CONF_PATH, 'r') as f:
        confs = toml.loads(f.read())
        src_conf = confs['Sources'][code_name]
        Resource(USERNAME=src_conf['USERNAME'])  # For safety
        Resource(PASSWORD=src_conf['PASSWORD'])  # For safety
        Config(API_HOST=src_conf['API_HOST'],
               KAFKA_BROKER=confs['Kafka']['BROKER'],
               KAFKA_TOPIC=confs['Kafka']['Topics'][code_name]['name'],
               EARLIEST_DATE=src_conf['EARLIEST_DATE'],
               DATE_FORMAT=src_conf['DATE_FORMAT'])
    # Configure the logger
    initialize_logger(logto=Configs.LOGTO,
                      prefix='extract_' + Configs.CODE_NAME,
                      timestamp=timestamp(),
                      debugging=Configs.DEBUG)
    # Open up the cache database
    Resource(Cache=DatabaseHandler(Configs.CACHE_PATH,
             table=cache_table, flag='c', autocommit=True))
