####################################################
# PACKAGE:  src.extract.transparent.airbnb_operational
# ENTRY:    __main__.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# This script extracts operational data of Airbnb.com listings from the data source provided by Lighthouse (formerly Transparent).
#
####################################################


from src.pipelines.transparent.airbnb_operational import *

# < EXTRACT, TRANSFORM, LOAD -------------------------------------------------------------

@handle(error_message=MSG_ERROR_UNEXPECTED, 
        strict=True, 
        exit_signal=2, 
        on_error=statistics_report)
def extract_opdata_per_date_range(
    start_date: datetime,
    end_date: datetime
) -> Generator[Dict, None, None]:
    extract_task = Task(
        Type="extract",
        Source=Configs.CODE_NAME,
        Metadata={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
    )

    @handle(error_message=MSG_ERROR_UNEXPECTED,
            strict=False,
            do_raise=False,
            timeout_secs=5,
            max_retries=3,
            fallback_value=([], extract_task.to_failed_task()))
    def _extract_opdata() -> List[Dict]:
        """
        Make sure we get results from the API. Adds extra layer of durability. 
        Retry Policy:
            - 3 retries
            - 5 ** retry between retries => 5, 25, 125 seconds
            - in case of failure, return a fallback value failed status
        """
        payload = {
            "export-format": "json",
            "type": "date/all-options",
            "value": f"{date_to_string(start_date, Configs.DATE_FORMAT)}~{date_to_string(end_date, Configs.DATE_FORMAT)}",
            "target": [
                "dimension",
                ["template-tag", "pricing_date"]
            ]}
        url_parameters = quote_plus(json.dumps([payload]))
        # POST
        url = 'https://db.seetransparent.com/api/card/34969/query/json?parameters=' + url_parameters
        Resources.DEMAND_ENDPOINT.update_url(url)
        response = Resources.DEMAND_ENDPOINT.POST()
        return response.json(), extract_task

    # START EXTRACTING...
    results, status = _extract_opdata()

    # if for any reason, extraction failed log the details and go next.
    if isinstance(status, FailedTask):
        printlog("REJECT", "error", status.__dict__)
        STATISTICS['Failure']['Extract'] += len(results)
        return
    printlog("EXTRACT", "debug", status.__dict__)

    if not results:
        return

    STATISTICS['Success']['Extract'] += len(results)

    for opdata in results:
        yield from transform_opdata(opdata)


@handle(MSG_ERROR_SOURCE_TRANSFORM, 
        strict=True, 
        exit_signal=2, 
        on_error=statistics_report)
def transform_opdata(record: Dict) -> Generator[Dict, None, None]:
    transform_task = Task(
        Type="transform",
        Source=Configs.CODE_NAME,
        Metadata={
            "endpoint": ['Transparent Operational Endpoint'],
            "id": record.get('unified_id')
        }
    )

    @handle(error_message=MSG_ERROR_SOURCE_TRANSFORM,
            strict=False,
            do_raise=False,
            timeout_secs=3,
            max_retries=3,
            fallback_value=(None, transform_task.to_failed_task(extra=record)))
    def _transform_record(record: Dict) -> Dict:
        def fix_numeric(num):
            if isinstance(num, float):
                return num
            return None

        record['occupancy'] = fix_numeric(record['occupancy'])
        record['ADR'] = fix_numeric(record['ADR'])
        record['revenue'] = fix_numeric(record['revenue'])
        record['airbnb_id'] = record['unified_id'].replace('AIR', '')
        return record, transform_task

    transformed_record, status = _transform_record(record)

    # if for any reason, transformation failed log the details and go next.
    if isinstance(status, FailedTask):
        printlog("REJECT", "error", status.__dict__)
        STATISTICS['Failure']['Transform'] += 1
        return
    # Too much logs if you include below one
    # printlog("TRANSFORM", "debug", status.__dict__)

    STATISTICS['Success']['Transform'] += 1
    yield transformed_record


@trace(message=MSG_CONST_EXTRACT_RECORDS)
@handle(MSG_ERROR_SOURCE_EXTRACT, 
        strict=True, 
        do_raise=True, 
        on_error=statistics_report)
def extract_opdata():
    for interval in monthly_intervals(Configs.EXTRACT_START_DATE, Configs.EXTRACT_END_DATE):
        yield from extract_opdata_per_date_range(interval[0], interval[1])


# < MAIN FUNCTIONS -------------------------------------------------------------
@handle(error_message=MSG_ERROR_UNEXPECTED_PIPELINE,
        strict=True,
        exit_signal=1,
        on_error=statistics_report,)
# @notrace: no logger has been initialized at this point
def main():
    if load_records_to_kafka(records=extract_opdata(),
                             batch_key=timestamp(),
                             chunk_size=10000,
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
