####################################################
# PACKAGE:  src.extract.rapidapi.airbnb_reviews
# ENTRY:    __main__.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# This script extracts reviews of Airbnb.com listings from Rapidapi data provider.
#
####################################################

from src.pipelines.rapidapi.airbnb_reviews import *

# < EXTRACT, TRANSFORM, LOAD -------------------------------------------------------------

# NOTRACE: May be called recursively multiple times.

@trace(message=MSG_CONST_EXTRACT_REVIEWS)
@handle(error_message=MSG_ERROR_SOURCE_EXTRACT, 
        strict=True, 
        exit_signal=2, 
        on_error=statistics_report)
def extract_reviews() -> Generator[Dict, None, None]:
    listings = Resources.Cache.read("current")
    printlog(f"Loaded {len(listings)} Listings from Cache.", "info")
    STATISTICS["Extra"]["Available Listings"] = len(listings)
    for listing in listings:
        yield from extract_reviews_per_listing(listing, Configs.EXTRACT_START_DATE)
        STATISTICS["Extra"]['Processed Listings'] += 1

    Extractor()

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
