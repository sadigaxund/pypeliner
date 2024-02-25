####################################################
# PACKAGE:  src.extract.snowflake.flight_schedules
# ENTRY:    __main__.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# This script extracts flight schedules in and out of Saudi Arabia provided by OAG.
#
####################################################

from src.pipelines.snowflake.flight_schedules import *

# < ETL FUNCTIONS -------------------------------------------------------------

@handle(MSG_ERROR_SOURCE_EXTRACT, 
        strict=True, 
        exit_signal=2, 
        on_error=statistics_report)
def extract_schedules(start_date: datetime, end_date: datetime, limit=None):
    start_date_str = date_to_string(start_date, Configs.DATE_FORMAT)
    end_date_str = date_to_string(end_date, Configs.DATE_FORMAT)
    
    limit_str = f"LIMIT {limit}" if limit else "" 
    
    QUERY = f"""SELECT *
        FROM OAG_SCHEDULES.DIRECT_CUSTOMER_CONFIGURATIONS.AXELERATED_SCHED
        WHERE FILE_DATE >= '{start_date_str}' AND FILE_DATE <= '{end_date_str}'
        ORDER BY FILE_DATE ASC {limit_str};"""
    # GET EXISTING ROWS
    printlog('Running Snowflake Query', 'info', payload={'query': QUERY})

    if Configs.DEBUG:  # Reason: Quota is limited
        return

    Resources.SNOWFLAKE_CURSOR.execute(QUERY)  # Execute the query

    # Get the number of rows
    ROW_COUNT = Resources.SNOWFLAKE_CURSOR.rowcount
    printlog(
        f"Extracting {ROW_COUNT} Flight Schedules from the database.", "info")
    STATISTICS['Success']['Extract'] += ROW_COUNT

    # Get the schema
    COL_NAMES = [desc[0] for desc in Resources.SNOWFLAKE_CURSOR.description]
    printlog(f"Columns to be extracted: {COL_NAMES}", "info")

    # Get the rows
    for schedule in Resources.SNOWFLAKE_CURSOR:
        yield transform_schedule(schedule, COL_NAMES)


@handle(MSG_ERROR_SOURCE_TRANSFORM, 
        strict=True, 
        do_raise=True, 
        on_error=statistics_report)
def transform_schedule(row, schema):
    retval = {}
    for i in range(len(schema)):
        key = schema[i]
        val = row[i]

        if any([
            isinstance(val, datetime),
            isinstance(val, decimal.Decimal),
        ]):
            val = str(val)

        retval[key] = val

    STATISTICS['Success']['Transform'] += 1
    return retval


# < MAIN FUNCTIONS -------------------------------------------------------------

@handle(error_message=MSG_ERROR_UNEXPECTED_PIPELINE,
        strict=True,
        exit_signal=1,
        on_error=statistics_report,)
# @notrace: no logger has been initialized at this point
def main():
    schedules_generator = extract_schedules(Configs.EXTRACT_START_DATE, Configs.EXTRACT_END_DATE)
    
    if load_records_to_kafka(records=schedules_generator,
                             batch_key=timestamp(),
                             chunk_size=50000,
                             source_name=Configs.CODE_NAME,
                             kafka_topic=Configs.KAFKA_TOPIC,
                             kafka_broker=Configs.KAFKA_BROKER,
                             delivery_report=message_delivery_report,):
        statistics_report()  # Report one last time
        printlog(MSG_SUCCESSFUL_PIPELINE, "info")
    else:
        printlog(MSG_NOTSUCCESS_PIPELINE, "warning")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as e:
        printlog("Recieved Keyboard Interrupt. Stopping...", 'warning')
