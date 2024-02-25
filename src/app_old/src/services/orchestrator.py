####################################################
# PACKAGE:  src.services
# ENTRY:    apify.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# Orchestration service.
#
####################################################

from src.services import *


# Create an instance of the FastAPI class
service = FastAPI(title="Orchestrator",
                  description="A Service that manages the extraction & reporting layers and communication between them.",
                  version="3.4.0",
                  debug=False)

# Middleware to check if the request comes from an internal IP
# service.add_middleware(TrustedHostMiddleware, allowed_hosts=['ser6:6064'])
service.add_middleware(TrustedHostMiddleware, allowed_hosts=internal_ips)

@service.post("/submit")
async def submit(task: Task = Depends(Task.build)):
    debug(f"Submit: {task.type}#{task.id}")
    """ 
    Example:
        payload = {
            "id" : optional. e.g. set to manual_id/cron_id to track 
            "type": 'report | extract'
            "source":  google_maps | airbnb_reviews...
            "metadata" : {
                "provider" : rapidapi | apify | transparent | snowflake
                "schedule" : recurrent | bootstrap | custom,
                "dataset": "IdCkPsEuhWfEQh3cv" | optional, only needed for apify
            }
        }
    """

    endpoint = {
        'extract' : ExtractorSubmit,
        'report' : ReporterSubmit,
    }[task.type]
    
    return endpoint.POST_JSON_SAFE(task.dissolve())


def send_report_to_fyrefuse(task: Task, report=None):
    # TODO: Fix to send failed report
    if not report:
        # TODO: Maybe add here: search for broken pipeline_id using source_name and generate failed report
        raise ValueError("Report has not been provided!")
    
    FyrefuseSubmit = ApiEndpoint(
        base_url=f"http://{OPTS['Fyrefuse']['HOST']}:{OPTS['Fyrefuse']['PORT']}",
        endpoint='/fusilli/deployer/instance/sequential/starter/',
        headers={'Content-Type': 'application/json'},
        interval = 1,
        retries = 1
    )
    
    response = FyrefuseSubmit.POST_JSON_SAFE(report)
    task.metadata['report'] = report
    task.metadata['response'] = response
    record_task(task.withType(f'submit: fyrefuse'))

@service.post("/done")
def done(task: Task = Depends(Task.build)):
    debug(f"Done: {task.type}#{task.id}")
    _report = task.metadata.pop('report', FYREFUSE_PAYLOAD_SCHEMA)
    record_task(task.withType(f"done: {task.type}"))
    
    if task.type == 'extract':
        OrchestratorSubmit.POST_JSON_SAFE(task.withType('report').dissolve())
    elif task.type == 'report':
        send_report_to_fyrefuse(task, _report)
                
    return {service.title: "OK"}

#### GET METHODS ####

@service.get("/health")
def health_get():
    return {
        **ExtractorHealth.GET_JSON_SAFE(),
        **ReporterHealth.GET_JSON_SAFE(),
        **ApifyHealth.GET_JSON_SAFE(),
    }


@service.head("/healthy")
@service.get("/healthy")
async def health_check():
    return {service.title: "Up and Running!"}


@service.head("/health")
def health_get():
    endpoints = [
        ExtractorHealth,
        ReporterHealth,
        ApifyHealth,
    ]

    for endpoint in endpoints:
        response = endpoint.GET()
        response.raise_for_status()


def record_task(task: Task):
    task.metadata['timestamp'] = timestamp(DEFAULT_LONG_TIMESTAMP_FORMAT)
    tasks_list = CACHE.read('history')
    tasks_list.insert(0, task.dissolve())  # LIFO | Display Most Recent First
    tasks_deque = deque(tasks_list, maxlen=1000)
    new_tasks_list = list(tasks_deque)
    CACHE.write('history', new_tasks_list)


@service.post('/record')
def record(task: Task = Depends(Task.build)):
    record_task(task)
    return {service.title: "OK"}


@service.post("/clear")
def clear():
    CACHE.write("history", [])
    return {service.title: "OK"}


@service.get("/history")
def history():
    return CACHE.read('history')

@service.route("/{path:path}")
async def catch_all(path: str):
    raise HTTPException(status_code=404, detail="Not Found")




if __name__ == "__main__":
    try:
        host = OPTS[service.title]['HOST']
        port = OPTS[service.title]['PORT']
        uvicorn.run("src.services.orchestrator:service", 
                    host=host, 
                    port=port, 
                    workers=4, 
                    reload=False)
    except KeyboardInterrupt:
        print("Stop Signal Recieved!")
