####################################################
# PACKAGE:  src.extract.rapidapi.booking_listings
# ENTRY:    __main__.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# This script extracts details of Booking.com listings from Rapidapi data provider.
#
####################################################

from src.pipelines.rapidapi.booking_listings import *

# < EXTRACT, TRANSFORM, LOAD -------------------------------------------------------------

@handle(MSG_ERROR_SOURCE_EXTRACT, strict=True, exit_signal=2, on_error=statistics_report)
def extract_listings(
    page_number: int = 0
) -> Generator[Dict, None, None]:
    extract_task = Task(
        Type="extract",
        Source=Configs.CODE_NAME,
        Metadata={
            "endpoint": 'Properties Endpoint',
            "page_number": page_number
        }
    )

    @handle(
        error_message=MSG_ERROR_UNEXPECTED,
        strict=False,
        do_raise=False,
        timeout_secs=5,
        max_retries=3,
        fallback_value=([], extract_task.to_failed_task())
    )
    def _extract_listings() -> List[Dict]:
        """
        Make sure we get results from the API. Adds extra layer of durability. 
        Retry Policy:
            - 3 retries
            - 5 ** retry between retries => 5, 25, 125 seconds
            - in case of failure, return a fallback value failed status
        """
        Resources.PROPERTIES_ENDPOINT.update_parameters(page=str(page_number))
        response = Resources.PROPERTIES_ENDPOINT.GET()
        response.raise_for_status()
        return response.json().get("result", []), extract_task

    # START EXTRACTING...
    results, status = _extract_listings()

    # if for any reason, extraction failed log the details and go next.
    if isinstance(status, FailedTask):
        printlog("REJECT", "error", payload=status.__dict__)
        STATISTICS['Failure']['Extract'] += 1
        return
    printlog("EXTRACT", "debug", status.__dict__)

    STATISTICS['Extra']['Current Listings'] += len(results)

    if not results:
        return

    for index, listing in enumerate(results):
        # Send for loading to Kafka
        STATISTICS['Success']['Extract'] += 1
        # Safely transform listing: add necessary metadata
        yield from transform_listing(listing.copy())

    yield from extract_listings(page_number + 1)


@handle(MSG_ERROR_SOURCE_TRANSFORM, strict=True, do_raise=True, on_error=statistics_report)
def transform_listing(listing: Dict) -> Generator[Dict, None, None]:
    listing_id = listing.get("hotel_id")

    transform_task = Task(
        Type="transform",
        Source=Configs.CODE_NAME,
        Metadata={
            "endpoint": ['Metadata V1 Endpoint', 'Metadata V2 Endpoint'],
            "hotel_id": listing_id
        }
    )

    @handle(
        error_message=MSG_ERROR_SOURCE_TRANSFORM,
        strict=False,
        do_raise=False,
        timeout_secs=5,
        max_retries=3,
        # In case if fails, add results of extraction to log to recover later
        fallback_value=(None, transform_task.to_failed_task(extra=listing))
    )
    def _transform_listing(listing: Dict) -> Dict:
        Resources.METADATA_V1_ENDPOINT.update_parameters(hotel_id=listing_id)
        Resources.METADATA_V2_ENDPOINT.update_parameters(hotel_id=listing_id)
        listing.update({
            "__metadata": {
                "data": Resources.METADATA_V1_ENDPOINT.GET_JSON(),
                "details": Resources.METADATA_V2_ENDPOINT.GET_JSON()
            }
        })
        return listing, transform_task

    transformed_listing, status = _transform_listing(listing)

    # if for any reason, extraction failed log the details and go next.
    if isinstance(status, FailedTask):
        printlog("REJECT", "error", status.__dict__)
        STATISTICS['Failure']['Transform'] += 1
        return
    printlog("TRANSFORM", "debug", status.__dict__)

    STATISTICS['Success']['Transform'] += 1
    yield transformed_listing


# < MAIN FUNCTIONS -------------------------------------------------------------

@handle(error_message=MSG_ERROR_UNEXPECTED_PIPELINE,
         strict=True,
         on_error=statistics_report,
         exit_signal=1)
# @notrace: no logger has been initialized at this point
def main():
    STATISTICS['Extra']['Previous Listings'] = len(
        Resources.Cache.read("current"))
    delivery_report = partial(message_delivery_report,
                              lambda listing: listing['hotel_id'])
    
    if load_records_to_kafka(records=extract_listings(),
                             batch_key=timestamp(),
                             source_name=Configs.CODE_NAME,
                             kafka_topic=Configs.KAFKA_TOPIC,
                             kafka_broker=Configs.KAFKA_BROKER,
                             delivery_report=delivery_report,):
        printlog(MSG_SUCCESSFUL_PIPELINE, "info")
        statistics_report()  # Report one last time
        recache_listings()
    else:
        printlog(MSG_NOTSUCCESS_PIPELINE, "warning")
    printlog(f"Made {ApiEndpoint.get_usage()} calls to the API.", "info")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as e:
        printlog("Recieved Keyboard Interrupt. Stopping...", 'warning')
