####################################################
# PACKAGE:  src.services
# ENTRY:    apify.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# Apify Webhook Handler Service.
#
####################################################

from src.services import *

RUNNING_PROCESSES = []
TIMEOUT_LIMIT = 3600

# Create an instance of the FastAPI class
service = FastAPI(title="Apify",
                  description="A webhook handler for Apify webhooks. When a POST request is received from Apify Integration, the payload is parsed and loaded to Kafka.",
                  version="3.4.0",
                  debug=False)

# Middleware to check if the request comes from an internal IP
service.add_middleware(TrustedHostMiddleware, allowed_hosts=internal_ips)

@service.post("/run")
async def run(payload: Dict):
    """ 
    Example:
        payload = {
            'source': google_maps | airbnb_reviews...
            'schedule': recurrent | bootstrap | custom
        }
    """
    source = payload['source']
    schedule = payload['schedule']
    
    run_task = Task(
        ID=str(uuid.uuid4()),
        Type="run",
        Source="apify_" + source,
        Metadata={
            "source": source,
            "schedule": schedule,
            "timestamp": timestamp()
        }
    )
    OrchestratorRecord.POST_JSON_SAFE(run_task.dissolve())

    command = [sys.executable, "-O", "-m", "src.extract.apify.start_query"]
    command += ["--source", source]
    command += ["--schedule", schedule]
    
    subprocess.run(command, timeout=TIMEOUT_LIMIT, check=True)
    
    return {service.title: "OK"}

# < REST ENDPOINTS -------------------------------------------------------------
@service.post("/webhook")
async def handle_webhook(payload: Dict):
    try:
        extract_task = Task(
            Type='extract',
            Source=payload['source'],
            Metadata={
                "provider": "apify",
                "schedule": "recurrent",
                "dataset": payload['resource']['defaultDatasetId']
            }
        )
        return OrchestratorSubmit.POST_JSON_SAFE(extract_task.dissolve())
    except KeyError as e:
        return {"KeyError": e}


@service.head("/health")
@service.get("/health")
async def health_check():
    return {service.title: "Up and Running!"}


@service.route("/{path:path}")
async def catch_all(path: str):
    raise HTTPException(status_code=404, detail="Not Found")


# < MAIN FUNCTIONS -------------------------------------------------------------

if __name__ == "__main__":
    try:
        host = OPTS[service.title]['HOST']
        port = OPTS[service.title]['PORT']
        uvicorn.run("src.services.apify:service",
                    host=host,
                    port=port,
                    reload=False,
                    proxy_headers=True)
    except KeyboardInterrupt:
        print("Stop Signal Recieved!")
