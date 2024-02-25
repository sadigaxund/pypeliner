####################################################
# PACKAGE:  src.extract.snowflake
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
        Resource(USER=src_conf['USER'],
                 PASSWORD=src_conf['PASSWORD'],
                 ACCOUNT=src_conf['ACCOUNT'])  # For safety
        Config(KAFKA_BROKER=confs['Kafka']['BROKER'],
               KAFKA_TOPIC=confs['Kafka']['Topics'][code_name]['name'],
               EARLIEST_DATE=src_conf['EARLIEST_DATE'],
               DATE_FORMAT=src_conf['DATE_FORMAT'])
    # Configure the logger
    initialize_logger(logto=Configs.LOGTO,
                      prefix='extract_' + Configs.CODE_NAME,
                      timestamp=timestamp(),
                      debugging=Configs.DEBUG)

