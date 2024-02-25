import requests
from pypeliner.tags.process import processor

immutable=False    # decides if process affect the value or not
forgiving=False   # decides if process should be forgiving the error
fallback="BAD"
raise_error=True

headers = {
    "X-RapidAPI-Key": "14d96c6fc2msh7414df2fb7e40ddp10513djsn985426003f23",
    "X-RapidAPI-Host": "priceline-com-provider.p.rapidapi.com"
}

@processor
def add_hotel_details_v1(hotel):
    url = "https://priceline-com-provider.p.rapidapi.com/v1/hotels/details"

    querystring = {"hotel_id": hotel['hotelid_t']}

    response = requests.get(url, headers=headers, params=querystring)

    metadata_v1 = response.json()
    
    return {
        "hotel_data": hotel,
        "metadata_v1": metadata_v1
    }

@processor
def add_hotel_details_v2(hotel):

    url = "https://priceline-com-provider.p.rapidapi.com/v2/hotels/details"

    querystring = {
        "hotel_id": hotel['hotel_data']['hotelid_ppn'],
        "videos":"true",
        "important_info":"true",
        "recent":"true",
        "photos":"true",
        "plugins":"true",
        "reviews":"false",
        "promo":"true",
        "guest_score_breakdown":"true",
        "nearby":"true",
        "id_lookup":"false"
    }

    response = requests.get(url, headers=headers, params=querystring)
    
    metadata_v2 = response.json()["getHotelHotelDetails"]["results"]["hotel_data"]["hotel_0"]
    
    return {
        **hotel,
        'metadata_v2': metadata_v2
    }

@processor
def add_booking_details(hotel):
    url = "https://priceline-com-provider.p.rapidapi.com/v1/hotels/booking-details"
    print(hotel.keys())
    querystring = {
        "hotel_id": hotel['hotel_data']['hotelid_t'],
        "date_checkout": "2060-12-31",
        "date_checkin": "2050-01-31",
    }

    response = requests.get(url, headers=headers, params=querystring)
    
    booking_details = response.json()

    return {
        **hotel,
        'booking_details': booking_details
    }


# FIXME: processors are applied alphabetically, make sure:
# 1. either to keep sequence correct: more desirable
# 2. or single processor per file: more easier