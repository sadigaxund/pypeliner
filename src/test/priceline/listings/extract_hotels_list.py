from pypeliner.tags.extract import execute, iterate, available
import queue
import requests

url = "https://priceline-com-provider.p.rapidapi.com/v2/hotels/downloadHotels"

QUERY = {"country_code": "sa", "limit": 100, "resume_key": ""}

headers = {
    "X-RapidAPI-Key": "14d96c6fc2msh7414df2fb7e40ddp10513djsn985426003f23",
    "X-RapidAPI-Host": "priceline-com-provider.p.rapidapi.com",
}

LAST_RESUME_KEY = ""

@execute
def run():
    global LAST_RESUME_KEY, QUERY
    response = requests.get(url, headers=headers, params=QUERY)
    js_resp = response.json()
    results = js_resp["getSharedBOF2.Downloads.Hotel.Hotels"]["results"]
    LAST_RESUME_KEY = results["resume_key"]
    return list(results["hotels"].values())


@available
def has_next():
    return LAST_RESUME_KEY is not None and LAST_RESUME_KEY != ""


@iterate
def iterate():
    global QUERY
    QUERY['resume_key'] = LAST_RESUME_KEY
