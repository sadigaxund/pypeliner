####################################################
# PACKAGE:  src.extract.rapidapi.hotels_reviews
# ENTRY:    __main__.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# This script extracts reviews of Hotels.com listings from Rapidapi data provider.
#
####################################################

from src.pipelines.rapidapi.hotels_reviews import *

# < EXTRACT, TRANSFORM, LOAD -------------------------------------------------------------

@handle(error_message=MSG_ERROR_EXTRACTING_REVIEWS, 
        strict=True, 
        exit_signal=2, 
        on_error=statistics_report)
# NOTRACE: May be called recursively multiple times.
def extract_reviews_per_listing(
    listing_id: Union[int, str],
    page_number: int = 1
) -> Generator[Dict, None, None]:
    extract_task = Task(
        Type="extract",
        Source=Configs.CODE_NAME,
        Metadata={
            "listing_id": listing_id,
            "page_number": page_number,
            "start_date": Configs.EXTRACT_START_DATE,
            "end_date": Configs.EXTRACT_END_DATE,
        }
    )

    @handle(error_message=MSG_ERROR_SOURCE_EXTRACT,
            strict=False,
            do_raise=False,
            timeout_secs=5,
            max_retries=3,
            fallback_value=([], extract_task.to_failed_task()))
    def _extract_reviews() -> List[Dict]:
        """
        Make sure we get results from the API. Adds extra layer of durability. 
        Retry Policy:
            - 3 retries
            - 5 ** retry between retries => 5, 25, 125 seconds
            - in case of failure, return a fallback value failed status
        """
        _date_time = date_to_string(Configs.EXTRACT_START_DATE, Configs.DATE_FORMAT)
        Resources.REVIEWS_ENDPOINT.update_parameters(
            hotel_id=listing_id,
            date_time=_date_time,
            page_number=page_number
        )
        response = Resources.REVIEWS_ENDPOINT.GET_JSON()
        return response.get("reviewInfo", {}).get("reviews", []), extract_task

    # START EXTRACTING...
    results, status = _extract_reviews()

    # if for any reason, extraction failed log the details and go next.
    if isinstance(status, FailedTask):
        printlog("REJECT", "error", payload=status.__dict__)
        STATISTICS["Failure"]["Extract"] += 1
        return
    printlog("EXTRACT", "debug", status.__dict__)

    STATISTICS["Extra"]["Extracted Reviews"] += len(results)

    if not results:
        return

    REACHED_END = False  # need just for STATISTICS & usage limitation
    # Filter reviews by date
    for index, review in enumerate(results):
        review_date_str = review['submissionTimeLocalized']
        review_date = datetime.strptime(review_date_str, "%b %d, %Y")
        if review_date > Configs.EXTRACT_END_DATE:
            STATISTICS["Extra"]["Discarded Reviews"] += 1
            continue

        if review_date < Configs.EXTRACT_START_DATE:
            STATISTICS["Extra"]["Discarded Reviews"] += 1
            # NOTE: This 'REACHED_END' holds true assuming source returns SORTED, DESC reviews based on date.
            # Otherwise switch to 'continue' to be safe, however, significantly more quota usage.
            REACHED_END = True
            continue

        # Add listing information and yield the review
        STATISTICS["Success"]["Extract"] += 1

        # TRANSFORM REVIEWS
        review['review_id'] = review.pop('id', None)
        review['hotel_id'] = listing_id
        STATISTICS["Success"]["Transform"] += 1

        yield review

    if not REACHED_END:
        yield from extract_reviews_per_listing(
            listing_id,
            page_number + 1
        )


@trace(message=MSG_STM_EXT_REVIEWS)
@handle(error_message=MSG_ERROR_EXTRACTING_REVIEWS, 
        strict=True, 
        exit_signal=2, 
        on_error=statistics_report)
def extract_reviews() -> Generator[Dict, None, None]:
    listings = Resources.Cache.read('current')
    printlog(f"Loaded {len(listings)} Listings from Cache.", "info")
    STATISTICS["Extra"]["Available Listings"] = len(listings)
    for listing in listings:
        yield from extract_reviews_per_listing(listing)
        STATISTICS["Extra"]['Processed Listings'] += 1


# < MAIN FUNCTIONS -------------------------------------------------------------


@handle(error_message=MSG_ERROR_UNEXPECTED_PIPELINE,
        strict=True,
        exit_signal=1,
        on_error=statistics_report,)
# @notrace: no logger has been initialized at this point
def main():
    if load_records_to_kafka(records=extract_reviews(),
                             batch_key=timestamp(),
                             source_name=Configs.CODE_NAME,
                             kafka_topic=Configs.KAFKA_TOPIC,
                             kafka_broker=Configs.KAFKA_BROKER,
                             delivery_report=message_delivery_report,):
        printlog(MSG_SUCCESSFUL_PIPELINE, "info")
        statistics_report()  # Report one last time
    else:
        printlog(MSG_NOTSUCCESS_PIPELINE, "warning")
    printlog(f"Made {ApiEndpoint.get_usage()} calls to the API.", "info")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as e:
        printlog("Recieved Keyboard Interrupt. Stopping...", 'warning')
