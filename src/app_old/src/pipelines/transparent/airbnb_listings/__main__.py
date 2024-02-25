####################################################
# PACKAGE:  src.extract.transparent.airbnb_listings
# ENTRY:    __main__.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# This script extracts details of Airbnb.com listings from the data source provided by Lighthouse (formerly Transparent).
#
####################################################

#!/usr/bin/python3.8
# -*- coding: utf-8 -*-

from src.pipelines.transparent.airbnb_listings import *

# < AUXILARY FUNCTIONS -------------------------------------------------------------


def fix_columns(data: Dict):
    fixed_columns = list(data.keys())
    # modify schema
    # 1. lowercase
    fixed_columns = [column.lower() for column in fixed_columns]
    # 2. remove spaces
    fixed_columns = [column.replace(' ', '_') for column in fixed_columns]
    return fixed_columns


def replace_keys(data: Dict, fixed_columns: List):
    # replace keys with the modified schema
    return {fixed_columns[i]: list(data.values())[i] for i in range(len(fixed_columns))}


# < EXTRACT, TRANSFORM, LOAD -------------------------------------------------------------
def extract_listings(extract_date):
    printlog(f"Extracting listings at {extract_date}", "info")

    extract_task = Task(
        Type="extract",
        Source=Configs.CODE_NAME,
        Metadata={
            "extract_date": extract_date,
        }
    )
    
    @handle(error_message=MSG_ERROR_UNEXPECTED,
            strict=False,
            do_raise=False,
            timeout_secs=5,
            max_retries=3,
            fallback_value=([], extract_task.to_failed_task()))
    def _extract_listings() -> List[Dict]:
        """
        Make sure we get results from the API. Adds extra layer of durability. 
        Retry Policy:
            - 3 retries
            - 5 ** retry between retries => 5, 25, 125 seconds
            - in case of failure, return a fallback value failed status
        """
        payload = {
            "export-format": "json",
            "type": "query",
            "value": extract_date,
            "query": {
                
            }
        }

        url_parameters = quote_plus(json.dumps([payload]))
        # POST
        url = 'https://db.seetransparent.com/api/card/35176/query/json?query=' + url_parameters
        
        Resources.DEMAND_ENDPOINT.update_url(url)
        response = Resources.DEMAND_ENDPOINT.POST()
        return response.json(), extract_task

    # START EXTRACTING...
    results, status = _extract_listings()

    # if for any reason, extraction failed log the details and go next.
    if isinstance(status, FailedTask):
        printlog("REJECT", "error", status.__dict__)
        STATISTICS['Failure']['Extract'] += len(results)
        return
    
    print(results)
    
    printlog("EXTRACT", "debug", status.__dict__)

    STATISTICS['Success']['Extract'] = len(results)

    first_listing = results[0]
    fixed_columns = fix_columns(first_listing)
    
    for listing in results:
        # FILTER
        # middle of month because that's when the data is updated
        str_date_field = listing['Month'] 
        
        if str_date_field != extract_date:
            STATISTICS['Extra']['Discarded'] += 1
            continue
        
        
        # TRANSFORM
        # replace keys with the modified schema
        listing = replace_keys(listing, fixed_columns)
        # save airbnb_id without the prefix
        listing['airbnb_id'] = listing['property_id'].replace('AIR', '')
        STATISTICS['Success']['Transform'] += 1
        # print(listing)
        yield listing


# < MAIN FUNCTIONS -------------------------------------------------------------


@handle(MSG_ERROR_UNEXPECTED_PIPELINE,
        strict=True,
        exit_signal=1,
        on_error=statistics_report,)
# @notrace: no logger has been initialized at this point
def main():
    if load_records_to_kafka(records=extract_listings(date_to_string(Configs.EXTRACT_START_DATE, "%Y-%m")),
                             batch_key=timestamp(),
                             chunk_size=5000,
                             source_name=Configs.CODE_NAME,
                             kafka_topic=Configs.KAFKA_TOPIC,
                             kafka_broker=Configs.KAFKA_BROKER,
                             delivery_report=message_delivery_report,):
        printlog(MSG_SUCCESSFUL_PIPELINE, "info")
        statistics_report()  # Report one last time
    else:
        printlog(MSG_NOTSUCCESS_PIPELINE, "warning")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as e:
        printlog("Recieved Keyboard Interrupt. Stopping...", 'warning')
