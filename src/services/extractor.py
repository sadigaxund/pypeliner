from fastapi import FastAPI, Request
import json
# Create an instance of FastAPI
app = FastAPI()

sources = [
    # "apify_google_maps",
    # "apify_tripadvisor_reviews",
    # "apify_tripadvisor_listings",
    
    # "transparent_airbnb_operational",
    # "transparent_airbnb_listings",
    
    # "oag_flight_schedules",
]
class APINotFound(Exception):...



@app.post("/query/new/{source}")
@app.get("/query/new/{source}")
async def enqueue(source: str, request: Request):
    
    api = None
    match source:
        case "rapidapi_airbnb_reviews":
            ...
        case "rapidapi_booking_reviews":
            ...
        case "rapidapi_booking_listings":
            ...
        case "rapidapi_priceline_reviews":
            ...
        case "rapidapi_priceline_listings":
            ...
        case "rapidapi_hotels_reviews":
            ...
        case "rapidapi_hotels_listings":
            ...
        case _:
            return {"error": f"Couldn't match Source=[{source}] to any API!"}
    
    parameters = None
    try:
        # Accessing the request body
        body = await request.body()
        parameters = json.loads(body)
    except json.decoder.JSONDecodeError as e:
        return {"error": "invalid json format!"}
    
    
    # 
    
    return {source: parameters}
