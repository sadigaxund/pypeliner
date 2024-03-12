import requests
from typing import Any, Iterable, NoReturn
from src.cores.implement.ingress import Core, Type


class DownloadHotels(Core, Type=Type.FUNCTION):
    
    def constructor(self):
        # Open database
        self.url = "https://priceline-com-provider.p.rapidapi.com/v2/hotels/downloadHotels"
        self.querystring = {
            "country_code": "sa", 
            "limit": "500"
        }
        self.resume_key = True
        self.headers = {
            "X-RapidAPI-Key": "",
            "X-RapidAPI-Host": "priceline-com-provider.p.rapidapi.com",
        }
        
    def destructor(self, exc_type, exc_value, traceback):
        # Close database
        return super().destructor(exc_type, exc_value, traceback)
    
    def available(self) -> bool:
        print("Available")
        return not (self.resume_key == None or self.resume_key == '')
    
    def iterate(self) -> NoReturn:
        print("Iterate")
        self.querystring['resume_key'] = self.resume_key
    
    def produce(self) -> Iterable | Any:
        print("Produce")
        response = requests.get(self.url, headers=self.headers, params=self.querystring)
        json_response = response.json()
        self.resume_key = json_response["getSharedBOF2.Downloads.Hotel.Hotels"]["results"]["resume_key"]
        results = json_response["getSharedBOF2.Downloads.Hotel.Hotels"]["results"]["hotels"]
        results = list(results.values()) # list of hotels
        yield from results
    


with DownloadHotels(flatten=True) as source:
    cnt = 0
    for result in source:
        cnt += 1
        print(cnt)
        


