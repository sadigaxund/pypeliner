####################################################
# PACKAGE:  src.services
# ENTRY:    apify.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# Extration starter service.
#
####################################################

from multiprocessing import process
from urllib import response
from src.services import *

# Create an instance of the FastAPI class
service = FastAPI(title="Extractor",
                  description="A Service that starts extraction process and communicates the results back to Orchestrator",
                  version="3.4.0",
                  debug=False)

# Middleware to check if the request comes from an internal IP
service.add_middleware(TrustedHostMiddleware, allowed_hosts=internal_ips)

RUNNING_PROCESSES = []
EXTRACT_TIMEOUT_LIMIT = 85000   # ~ 23.5 hours

def build_extract_command(payload: Task):
    command = [sys.executable, "-O", "-m"]
    source = payload.source
    metadata = payload.metadata
    provider = metadata['provider']
    
    module = f"src.extract.{provider}"
    
    if provider == 'apify':
        command.append(module + '.extract_results')
        command.append('--source')
        command.append(source)
        command.append('--dataset')
        command.append(metadata['dataset'])
    else:
        command.append(f"{module}.{source}")
        command.append('--schedule')
        command.append(metadata['schedule'])
        
    command.append('--out')
    command.append(OPTS['Extractor']['LOGS'])
    return command

def start_job(task: Task):
    RUNNING_PROCESSES.append(task.dissolve())
    log = None
    try:
        result = subprocess.run(build_extract_command(task),
                                timeout=EXTRACT_TIMEOUT_LIMIT,
                                check=True,
                                stdout=subprocess.PIPE,
                                text=True)
        
        log = result.stdout
        task.metadata['grace'] = 1
        task.metadata['status'] = 1
    except subprocess.CalledProcessError as e:
        log = e.output
        task.metadata['grace'] = 1
        task.metadata['status'] = 0
    except subprocess.TimeoutExpired as e:
        task.metadata['grace'] = 0
        task.metadata['status'] = 0
        print(f"Extraction did not finish on time: {str(e)}")
    except Exception as e3:
        task.metadata['grace'] = 0
        task.metadata['status'] = 0
        print(f"Extraction subprocess failed due to: {str(e3)}")
    finally:
        task.metadata['timestamp'] = timestamp()
        task.metadata['log'] = log
        OrchestratorDone.POST(task.dissolve())
        if task in RUNNING_PROCESSES:
            RUNNING_PROCESSES.remove(task)


@service.post("/submit")
async def submit(task: Task = Depends(Task.build)):
    # Start a new thread for the background task
    threading.Thread(target=start_job, args=(task,)).start()
    OrchestratorRecord.POST_JSON_SAFE(task.withType(f"submit: {task.type}").dissolve())
    return {service.title: "OK"}


@service.get("/status")
def status():
    return {"Running Processes": RUNNING_PROCESSES}

@service.get("/health")
def root():
    return {service.title: "Up and Running!"}

@service.route("/{path:path}")
async def catch_all(path: str):
    raise HTTPException(status_code=404, detail="Not Found")


if __name__ == "__main__":
    try:
        host = OPTS[service.title]['HOST']
        port = OPTS[service.title]['PORT']
        uvicorn.run("src.services.extractor:service",
                    host=host,
                    port=port,
                    reload=False,
                    proxy_headers=True)
    except KeyboardInterrupt:
        print("Stop Signal Recieved!")
