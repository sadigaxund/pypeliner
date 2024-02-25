from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from ratelimiter import RateLimiter
from datetime import datetime
from pathlib import Path
from typing import Dict

from .logs import printlog
from .sql import DatabaseHandler, CacheReadException, CacheWriteException
import validators
import requests
import calendar
import json

__all__ = [
    "ApiEndpoint"
]

class ApiEndpoint:
    __USAGE = 0
    __LIMIT = float('inf')
    __RATE_LIMITER = RateLimiter(max_calls=10, period=1)
    DATABASE = None

    def __init__(self, base_url='', endpoint=None, parameters=None, headers=None, data=None, name="RapidAPI Endpoint", retries=5, interval=30, forcelist=[400, 401, 403, 429, 500, 502, 503, 504]):
        self.name = name
        self.base_url = base_url
        self.endpoint = endpoint if endpoint is not None else ""
        self.parameters = parameters if parameters is not None else {}
        self.headers = headers if headers is not None else {}
        self.set_payload(data)
        
        self.retries = retries
        self.interval = interval
        self.forcelist = forcelist
    
    def set_payload(self, data: dict):
        if data is not None:
            try:
                from .functions import custom_encoder
                self.data = json.dumps(data, default=custom_encoder)
            except:
                self.data = data
        else:
            self.data = {}
    
    @classmethod
    def get_quota(cls):
        if cls.DATABASE is None:
            raise CacheReadException("Quota file has not been set!")

        return cls.DATABASE.read('quota')

    @classmethod
    def update_quota(cls) -> bool:
        # Load the quota information from the database
        quota = None
        try:
            quota = cls.get_quota()
        except:
            return False
        # Reset usage if the current day is the same as the day of the month when the quota was renewed
        if datetime.now().day == quota['subscribe_date']['day']:
            quota['quota_used'] = 0
        else:
            quota['quota_used'] += cls.__USAGE

        cls.__USAGE = 0

        # Write the updated JSON content back to database
        cls.DATABASE.write('quota', quota)
        
        return True

    @classmethod
    def set_quota(cls, database: DatabaseHandler, unlimited: bool) -> Dict:
        cls.DATABASE = database
        quota = cls.get_quota()
        requests_per_second = quota.get("requests_per_second", 5)
        refresh_day = quota.get("subscribe_date").get("day")
        remaining_quota = quota.get("monthly") - quota.get("quota_used")
        remaining_days = days_until_renewal(
            current_date=datetime.now(),
            renewal_day=refresh_day
        )
        cls.__LIMIT = calculate_daily_quota(
            remaining_quota=remaining_quota,
            remaining_days=remaining_days
        ) if not unlimited else float('inf')
        cls.__RATE_LIMITER = RateLimiter(max_calls=requests_per_second, period = 1)
        
        printlog("API Client Details", "info", {
            "Requests Per Second": requests_per_second,
            "Refresh Day": refresh_day,
            "Days Before Refresh": remaining_days,
            "Remaining Quota": str(remaining_quota),
            "Daily Quota": str(cls.__LIMIT)
        })
        # Return the quota information
        return True

    @classmethod
    def set_requests_per_second(cls, requests: int, period: int = 1):
        cls.__RATE_LIMITER = RateLimiter(max_calls=requests, period=period)

    @classmethod
    def get_usage(cls):
        return cls.__USAGE
    
    def update_parameters(self, **params) -> 'ApiEndpoint':
        self.parameters.update(params)
        return self

    def update_headers(self, new_headers: dict) -> 'ApiEndpoint':
        self.headers.update(new_headers)
        return self
    
    def update_url(self, url:str) -> 'ApiEndpoint':
        self.base_url = url
        return self
    
    def get_full_url(self):
        return self.base_url + self.endpoint
    
    def _validate_url(self):
        url = self.base_url + self.endpoint
        if not validators.url(url):
            raise ValueError(f"Invalid URL: {url}")

    def _check_usage_limit(self):
        if ApiEndpoint.__USAGE >= ApiEndpoint.__LIMIT:
            raise Exception(f"Quota of {ApiEndpoint.__LIMIT} exceeded.")

    def GET(self):
        return self._MAKE('GET')
    def GET_CODE(self):
        return self.GET().status_code
    def GET_JSON(self) -> Dict:
        return self.GET().json()

    def GET_JSON_SAFE(self, fallback_key=None) -> Dict:
        fallback_key = fallback_key or self.name
        try:
            response = self.GET()
            response.raise_for_status()
            try:
                return response.json()
            except requests.exceptions.JSONDecodeError as e:
                return {fallback_key: response.text}
        except Exception as rest:
            return {fallback_key: str(rest)}
    
    def POST(self, payload = None):
        if payload:
            self.set_payload(payload)
        
        return self._MAKE('POST')

    def POST_CODE(self, payload=None):
        return self.POST(payload).status_code

    def POST_JSON(self, payload=None) -> Dict:
        return self.POST(payload).json()
    
    def POST_JSON_SAFE(self, payload=None,  optional_key=None) -> Dict:
        optional_key = optional_key or self.name
        try:
            response = self.POST(payload)
            response.raise_for_status()
            try:
                return response.json()
            except requests.exceptions.JSONDecodeError as e:
                return {optional_key: response.text}
        except Exception as rest:
            return {optional_key: str(rest)}
    
    def PUT(self):
        return self._MAKE('PUT')
    def PUT_CODE(self):
        return self.PUT().status_code

    def PUT_JSON(self) -> Dict:
        return self.PUT().json()
    def DELETE(self):
        return self._MAKE('DELETE')
    def DELETE_CODE(self):
        return self.DELETE().status_code

    def DELETE_JSON(self) -> Dict:
        return self.DELETE().json()
    def PATCH(self):
        return self._MAKE('PATCH')
    def PATCH_CODE(self):
        return self.PATCH().status_code

    def PATCH_JSON(self) -> Dict:
        return self.PATCH().json()
    def OPTIONS(self):
        return self._MAKE('OPTIONS')
    def OPTIONS_CODE(self):
        return self.OPTIONS().status_code

    def OPTIONS_JSON(self) -> Dict:
        return self.OPTIONS().json()
    def HEAD(self):
        return self._MAKE('HEAD')
    def HEAD_CODE(self):
        return self.HEAD().status_code

    def HEAD_JSON(self) -> Dict:
        return self.HEAD().json()

    def _MAKE(self, method: str):
        url = self.base_url + self.endpoint

        # self._validate_url()
        self._check_usage_limit()
        """
            RETRY POLICY:
            -------------
            Given a backoff factor of 30 seconds and a maximum retry count of 3:

            1. First retry delay: [1 * 30 = 30] seconds
            2. Second retry delay: [2 * 30 = 60] seconds
            3. Third retry delay: [4 * 30 = 120] seconds

            Summing these up: [30 + 60 + 120 = 210] seconds in total delays due to retries.

            For the request durations with a timeout of 180 seconds:

            - Initial request
            - 1st retry (after 30 seconds)
            - 2nd retry (after 60 more seconds)
            - 3rd retry (after 120 more seconds)

            With each request potentially taking up to 180 seconds to complete, the total maximum duration would be:

            4 * 180 + 210 = 930 seconds or 15 minutes and 30 seconds.

            * 15.5 minutes before failing.
        """
        with requests.Session() as session, ApiEndpoint.__RATE_LIMITER:
            # define the retry policy
            retry = Retry(total=self.retries,
                          connect=self.retries,
                          status_forcelist=self.forcelist,
                          backoff_factor=self.interval)
                # read=3,
                # redirect=0,
            # Mount the adapter with the retry policy
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            # Function mapping
            
            # Send the request
            response = session.request(
                method,
                url, 
                params=self.parameters, 
                headers=self.headers, 
                data=self.data,
                timeout=(180, 300))
            ApiEndpoint.__USAGE += 1
        return response

def calculate_daily_quota(remaining_quota, remaining_days):
    if remaining_days == 0 or remaining_quota == float('inf'):
        return remaining_quota

    return remaining_quota // remaining_days + remaining_quota % remaining_days


def days_until_renewal(current_date: datetime, renewal_day: int):
    # get current day number
    current_day = current_date.day
    
    if current_day <= renewal_day:
        return renewal_day - current_day
    else:
        # get last day of this month
        last_day_of_month = calendar.monthrange(current_date.year, current_date.month)[1]
        remaining = last_day_of_month - current_day + renewal_day
        return remaining


