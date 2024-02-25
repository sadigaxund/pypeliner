####################################################
# PACKAGE:  src.extract.rapidapi.priceline_listings
# ENTRY:    __main__.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# This script extracts details of Priceline.com listings from Rapidapi data provider.
#
####################################################


from src.pipelines.rapidapi.priceline_listings import *

# < EXTRACT, TRANSFORM, LOAD -------------------------------------------------------------


@handle(MSG_ERROR_EXTRACTING_LISTINGS, 
        strict=True, 
        exit_signal=2, 
        on_error=statistics_report)
def extract_listings(
    batch_size: int = Configs.PAGE_SIZE,
    resume_key: str = ''
) -> Generator[Dict, None, None]:

    extract_task = Task(
        Type="extract",
        Source=Configs.CODE_NAME,
        Metadata={
            'endpoint': 'Properties Endpoint',
            "batch_size": batch_size,
            "resume_key": resume_key,
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
        # Send the request
        Resources.LISTINGS_ENDPOINT.update_parameters(
            limit=batch_size,
            resume_key=resume_key
        )
        response = Resources.LISTINGS_ENDPOINT.GET_JSON()
        # Process the response
        response = response\
            .get('getSharedBOF2.Downloads.Hotel.Hotels', {})\
            .get('results', {})
        listings = response.get('hotels', {}).values()
        new_resume_key = response.get('resume_key', '')
        return listings, new_resume_key, extract_task

    results, new_resume_key, status = _extract_listings()
    STATISTICS['Extra']['Last Resume Key'] = new_resume_key

    # if for any reason, extraction failed log the details and go next.
    if isinstance(status, FailedTask):
        printlog("REJECT", "error", payload=status.__dict__)
        STATISTICS['Failure']['Extract'] += 1
        return

    printlog("EXTRACT", "debug", status.__dict__)

    STATISTICS['Extra']['Current Listings'] += len(results)

    for index, listing in enumerate(results):
        # Safely transform listing: add necessary metadata
        STATISTICS['Success']['Extract'] += 1
        # Send for loading to Kafka
        yield from transform_listing(listing.copy())

    # If there are more hotels, recursively call the function
    if not any((new_resume_key == '', len(results) < batch_size)):
        yield from extract_listings(batch_size, new_resume_key)


@handle(MSG_ERROR_SOURCE_TRANSFORM, 
        strict=True, 
        do_raise=True, 
        on_error=statistics_report)
def transform_listing(listing: Dict) -> Generator[Dict, None, None]:
    transform_task = Task(
        Type="transform",
        Source=Configs.CODE_NAME,
        Metadata={
            "endpoint": ['Metadata V1 Endpoint', 'Metadata V2 Endpoint', 'Booking Details Endpoint'],
            "listing_id": {
                'hotelid_t': listing.get('hotelid_t'),
                'hotelid_ppn': listing.get('hotelid_ppn'),
            }
        }
    )

    @handle(error_message=MSG_ERROR_SOURCE_TRANSFORM,
            strict=False,
            do_raise=False,
            timeout_secs=5,
            max_retries=3,
            fallback_value=(None, transform_task.to_failed_task(extra=listing)))
    def _transform_listing(listing: Dict) -> Dict:
        # Get metadata v1
        Resources.METADATA_V1_ENDPOINT.update_parameters(
            hotel_id=listing.get('hotelid_t'))
        metadatav1: Dict = Resources.METADATA_V1_ENDPOINT.GET_JSON()
        # Get metadata v2
        Resources.METADATA_V2_ENDPOINT.update_parameters(
            hotel_id=listing.get('hotelid_ppn'))
        response: Dict = Resources.METADATA_V2_ENDPOINT.GET_JSON()
        metadatav2: Dict = response\
            .get('getHotelHotelDetails', {})\
            .get('results', {})\
            .get('hotel_data', {})\
            .get('hotel_0', {})
        # Get booking details
        Resources.BOOKING_DETAILS_ENDPOINT.update_parameters(
            hotel_id=listing.get('hotelid_t'))
        booking_details: Dict = Resources.BOOKING_DETAILS_ENDPOINT.GET_JSON()
        # Remap IDs
        mapped_id = {
            'hotel_id_ppn': coalesce(listing.get('hotelid_ppn'), metadatav2.get('id')),
            'hotel_id_a':   coalesce(listing.get('hotelid_a'),  metadatav2.get('id_a')),
            'hotel_id_b':   coalesce(listing.get('hotelid_b'),  metadatav2.get('id_b')),
            'hotel_id_t':   coalesce(listing.get('hotelid_t'),  metadatav2.get('id_t'), metadatav1.get('hotelId')),
            'hotel_id_r':   metadatav2.get('id_r'),
            'hotel_id_g':   metadatav2.get('id_g')
        }
        # Transform the property
        listing.update({
            "__metadata": {
                'mapped_id':    mapped_id,
                'metadata_v1':  metadatav1,
                'metadata_v2':  metadatav2,
                'booking_details': booking_details
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

@handle(error_message=MSG_ERROR_UNEXPECTED_PIPELINE + Configs.CODE_NAME,
        strict=True,
        exit_signal=1,
        on_error=statistics_report,)
# @notrace: no logger has been initialized at this point
def main():
    STATISTICS['Extra']['Previous Listings'] = len(Resources.Cache.read("current"))
    delivery_report = partial(message_delivery_report,
                              lambda listing: {
                                  "hotel_id_ppn": listing.get('hotelid_ppn'),
                                  "hotel_id_t": listing.get('hotelid_t')
                                })

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
