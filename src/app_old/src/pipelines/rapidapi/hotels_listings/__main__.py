####################################################
# PACKAGE:  src.extract.rapidapi.hotels_listings
# ENTRY:    __main__.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# This script extracts details of Hotels.com listings from Rapidapi data provider.
#
####################################################

from src.pipelines.rapidapi.hotels_listings import *

LISTINGS_DEDUPE_CACHE=set()
REGIONS_DEDUPE_CACHE=set()


# < EXTRACT, TRANSFORM, LOAD -------------------------------------------------------------

@handle(MSG_ERROR_SOURCE_EXTRACT, 
        strict=True, 
        exit_signal=2, 
        on_error=statistics_report)
# NOTRACE: Custom tracing is used
def extract_listings_per_region(
    region_id,
    page_number=1
) -> Generator[Dict, None, None]:

    extract_task = Task(
        Type="extract",
        Source=Configs.CODE_NAME,
        Metadata={
            "endpoint": 'Listings Endpoint',
            "region_id": region_id,
            "page_number": page_number,
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
        Resources.LISTINGS_ENDPOINT.update_parameters(
            region_id=region_id, page_number=page_number)
        response = Resources.LISTINGS_ENDPOINT.GET()
        response.raise_for_status()
        return response.json().get('properties', []), extract_task

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
        listing_id = listing.get('id')

        if listing_id in LISTINGS_DEDUPE_CACHE:
            STATISTICS['Extra']['Discarded Listings'] += 1
            continue

        LISTINGS_DEDUPE_CACHE.add(listing_id)
        # Safely transform listing: add necessary metadata
        STATISTICS['Success']['Extract'] += 1
        # Send for loading to Kafka
        yield from transform_listing(listing.copy())

    yield from extract_listings_per_region(region_id, page_number + 1)


@handle(MSG_ERROR_EXTRACTING_LISTINGS, 
        strict=True, 
        exit_signal=2, 
        on_error=statistics_report)
def extract_listings_per_query(
    query_string: str
) -> Generator[Dict, None, None]:
    extract_task = Task(
        Type="extract",
        Source=Configs.CODE_NAME,
        Metadata={
            "endpoint": "Regions Endpoint",
            "query": query_string,
        }
    )

    @handle(error_message=MSG_ERROR_EXTRACTING_REGIONS,
            strict=False,
            do_raise=False,
            timeout_secs=5,
            max_retries=3,
            fallback_value=([], extract_task.to_failed_task()))
    def _extract_regions() -> List[Dict]:
        Resources.REGIONS_ENDPOINT.update_parameters(query=query_string)
        response = Resources.REGIONS_ENDPOINT.GET()
        regions = response.json()['data']
        filtered_regions = []
        # filter
        for region in regions:
            country_info = region['hierarchyInfo']['country']
            if any([
                country_info.get('isoCode3') == 'SAU',
                country_info.get('isoCode2') == 'SA',
                country_info.get('name') == 'Saudi Arabia',
            ]) and all([
                region.get('gaiaId') != None,
                region.get('gaiaId', 0) not in REGIONS_DEDUPE_CACHE,
                region.get('type') == "CITY"
            ]):
                filtered_regions.append(region['gaiaId'])
                REGIONS_DEDUPE_CACHE.add(region['gaiaId'])
                STATISTICS['Extra']['Extracted Regions'] += 1
            else:
                STATISTICS['Extra']['Discarded Regions'] += 1
        return filtered_regions, extract_task

    # START EXTRACTING...
    results, status = _extract_regions()

    # if for any reason, extraction failed log the details and go next.
    if isinstance(status, FailedTask):
        printlog("REJECT", "error", payload=status.__dict__)
        STATISTICS['Failure']['Extract'] += 1
        return
    printlog("EXTRACT", "debug", status.__dict__)

    for region in results:
        yield from extract_listings_per_region(region)
        STATISTICS['Extra']['Transformed Regions'] += 1


@trace(message=MSG_CONST_EXTRACT_LISTINGS)
@handle(error_message=MSG_ERROR_SOURCE_EXTRACT, 
        strict=True, 
        do_raise=True, 
        on_error=statistics_report)
def extract_listings():
    queries = Resources.Cache.read('queries')
    STATISTICS['Extra']['Previous Listings'] = len(
        queries)  # Total Search Terms
    for query in queries:
        yield from extract_listings_per_query(query_string=query)


@handle(MSG_ERROR_SOURCE_TRANSFORM, 
        strict=True, 
        do_raise=True, 
        on_error=statistics_report)
def transform_listing(listing: Dict) -> Dict:
    listing_id = listing.get("id")
    transform_task = Task(
        Type="transform",
        Source=Configs.CODE_NAME,
        Metadata={
            "endpoint": ['Hotel Info Endpoint', 'Hotel Details Endpoint'],
            "hotel_id": listing_id
        }
    )

    @handle(error_message=MSG_ERROR_SOURCE_TRANSFORM,
            strict=False,
            do_raise=False,
            timeout_secs=1,
            max_retries=3,
            fallback_value=(None, transform_task.to_failed_task(extra=listing)))
    def _transform_listing(listing: Dict) -> Dict:
        Resources.INFO_ENDPOINT.update_parameters(hotel_id=listing_id)
        Resources.DETAILS_ENDPOINT.update_parameters(hotel_id=listing_id)
        listing.update({
            "__metadata": {
                "info": Resources.INFO_ENDPOINT.GET_JSON(),
                "details": Resources.DETAILS_ENDPOINT.GET_JSON(),
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
        exit_signal=1,
        on_error=statistics_report,)
# @notrace: no logger has been initialized at this point
def main():
    STATISTICS['Extra']['Previous Listings'] = len(
        Resources.Cache.read("current"))
    delivery_report = partial(message_delivery_report,
                              lambda listing: listing['id'])
    
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
