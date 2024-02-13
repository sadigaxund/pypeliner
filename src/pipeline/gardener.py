from typing import Deque
from seed import Seed
import requests
from collections import deque
from pprint import pprint as print

seeds: Deque[Seed] = deque()

def plant(seeds: Deque[Seed]):
    while seeds:
        seed = seeds.pop()
        
        seed.run()
        
        if seed.__has_next__():
            seeds.append(seed.__iterate__())
        
        
QUERY_STRING = {"country_code": "sa", "limit": "1000"}

def run():
    url = "https://priceline-com-provider.p.rapidapi.com/v2/hotels/downloadHotels"
    headers = {
        "X-RapidAPI-Key": "14d96c6fc2msh7414df2fb7e40ddp10513djsn985426003f23",
        "X-RapidAPI-Host": "priceline-com-provider.p.rapidapi.com"
    }
    return requests.get(url, headers=headers, params=QUERY_STRING)

def has_next(response: requests.Response):
    try:
        json_response: dict = response.json()
        if json_response.get('getSharedBOF2.Downloads.Hotel.Hotels', {})\
                .get('results', {}).get('resume_key'):
                        return True
        else:
            return False
    except requests.exceptions.JSONDecodeError:
        return False

def iterate(last_result: requests.Response):
    json_response = last_result.json() # Handle json parse
    QUERY_STRING['resume_key'] = json_response['getSharedBOF2.Downloads.Hotel.Hotels']['results']['resume_key']

hotels:set = set()
response = run()
for hotel in response.json()['getSharedBOF2.Downloads.Hotel.Hotels']['results']['hotels'].values():
    hotels.add(hotel['hotelid_ppn'])
print(f"Total Hotels: {len(hotels)}")
while has_next(response):
    print(QUERY_STRING)
    response = run()
    iterate(response)
    for hotel in response.json()['getSharedBOF2.Downloads.Hotel.Hotels']['results']['hotels'].values():
        hotels.add(hotel['hotelid_ppn'])
    print(f"Total Hotels: {len(hotels)}")

    
