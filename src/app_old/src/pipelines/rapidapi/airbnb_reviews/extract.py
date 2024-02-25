
from datetime import datetime
from typing import *
from src.utils.dates import date_to_string, to_datetime, most_recent
from src.utils.api import ApiEndpoint
from src.utils import test_connection
from src.utils.sql import DatabaseHandler
from src.utils.logs import printlog

class Extractor:
    
    def __init_attrs__(self, **kwargs):
        self.__dict__.update(kwargs)
    
    def __init_rapidapi__(self, 
                          api_key: str, 
                          api_host: str,
                          cache, unlimited=True):
        headers = {"X-RapidAPI-Key": api_key,
                   "X-RapidAPI-Host": api_host}
        
        self.endpoint = ApiEndpoint(
            name="Reviews Endpoint",
            base_url=f"https://{api_host}",
            endpoint="/v2/listingReviews",
            parameters={                            # Sort order: ASC
                "id": "619966061834034729",         # Example
                "date_time": "2023-01-11 10:52:50"  # Example
            },
            headers=headers
        )
        
        connectivity = ApiEndpoint(
            name="Connectivity Endpoint",
            base_url=f"https://{api_host}",
            endpoint="/v2",
            headers=headers
        )
        
        test_connection(connectivity)
        test_connection(self.endpoint)
        ApiEndpoint.set_quota(cache, unlimited)
        ...
    
    def __init__(self, 
                 api_key:str, 
                 api_host:str, 
                 schedule: Dict,
                 database: DatabaseHandler,
                 date_format: str,
                 counter:Dict = None) -> None:
        self.__init_attrs__(extract_start_date=schedule['start'],
                            extract_end_date=schedule['end'],
                            date_format=date_format)
        self.__init_rapidapi__(api_key, 
                               api_host, 
                               cache=database,
                               unlimited=schedule['type'] == 'bootstrap')
        
        # raises: KeyError if invalid counter object is passed
        self.counter = counter if counter and counter['Extract'] else {"Extract": 0}

    def __extract_reviews_per_listing(self, listing_id: str, start_date) -> List:
        if start_date > self.extract_end_date:
            return []
        
        date_time = date_to_string(start_date, self.date_format)
        self.endpoint.update_parameters(id=listing_id, date_time=date_time)
        response = self.endpoint.GET()
        response.raise_for_status()
        
        # No reviews were found. NOT AN ERROR.
        if response.get("error") == "No results":
            return []
        
        # If all went well, return results
        return response.json().get("results", [])


    def __extract_all_reviews_per_listing(self, listing_id: str, start_date: datetime, end_date: datetime) -> Generator[Dict, None, None]:
        
        while start_date <= end_date:
            reviews = self.__extract_reviews_per_listing(listing_id, start_date)
            self.counter['Extract'] += len(reviews)

            for review in reviews:
                review_date_str = review.get("date_time")
                review_date = to_datetime(review_date_str, self.date_format)
                
                if any(review_date < self.extract_start_date,
                       review_date > self.extract_end_date):
                    self.counter['Extract'] -= 1
                    continue
                
                review['airbnb_id'] = listing_id
                yield review
                
                # iterate...
                start_date = most_recent(start_date, review_date)

    def extract_listings(self):
        return self.database.read("current")
    
    def extract(self, listings: List[str]):
        printlog(f"Loaded {len(listings)} Listings from Cache.", "info")
        
        ...