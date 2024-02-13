from elements import SourceNode, InterimNode, SinkNode
from functools import partial
import requests

class Loader:
    def load(self, record):
        print(f"Loaded Record=[{record}]")


def extract(page_number = 1):
    url = "https://hotels-com-provider.p.rapidapi.com/v2/hotels/reviews/list"

    querystring = {
        "domain": "US",
        "locale": "en_US",
        "hotel_id": "1105156",
        "sort_order": "NEWEST_TO_OLDEST",
        "page_number": str(page_number),
    }

    headers = {
        "X-RapidAPI-Key": "14d96c6fc2msh7414df2fb7e40ddp10513djsn985426003f23",
        "X-RapidAPI-Host": "hotels-com-provider.p.rapidapi.com",
    }

    response = requests.get(url, headers=headers, params=querystring)
    
    reviews = response.json()["reviewInfo"]["reviews"]

    if not reviews:
        return
    
    yield from reviews
    yield from extract(page_number + 1)

def transform(record):
    return {
        'ID': record['id'],
        'Title': record['title'],
        'Text': record['text'],
        'Brand': record['brandType']
    }

loader = Loader()

def load(client, record):
    client.load(record)

source = SourceNode.SourceNode(extract())
transf = InterimNode.InterimNode(transform, input=source.output)
loader = SinkNode.SinkNode(partial(load, loader), transf.output)

ETL = source + transf + loader

loader.run(chunk_size=10, backoff_seconds=5)