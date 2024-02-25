####################################################
# PACKAGE:  src.services
# ENTRY:    apify.py
# AUTHOR:   Sadig Akhund
# COMPANY:  KDM Force Ltd
# DATE:     2023-12-15
# VERSION:  v3.4
#
# DESCRIPTION:
# Reporting starter service.
#
####################################################

from src.services import *

# Create an instance of the FastAPI class
service = FastAPI(title="Reporter",
                  description="A Service that summarizes the results of extraction process and communicates the results back to Orchestrator",
                  version="3.4.0",
                  debug=False)

# Middleware to check if the request comes from an internal IP
service.add_middleware(TrustedHostMiddleware, allowed_hosts=internal_ips)

RUNNING_PROCESSES = []
EXTRACT_TIMEOUT_LIMIT = 85000   # ~ 23.5 hours

def build_report_command(task: Task):
    command = [sys.executable, "-O", "-m", 'src.report']
    
    if task.metadata['grace'] == 1:
        command += ['--grace']
    
    if task.metadata['status'] == 1:
        command += ['--status']

    command += ['--provider', task.metadata['provider']]
    command += ['--source', task.source]
    command += ['--log', task.metadata.pop('log')]
    command += ['--logs', 'logs/']
    
    return command

def start_job(task: Task):
    RUNNING_PROCESSES.append(task.dissolve())
    try:
        result = subprocess.run(build_report_command(task),
                                timeout=EXTRACT_TIMEOUT_LIMIT,
                                check=True,
                                stdout=subprocess.PIPE, 
                                text=True)
        
        report = json.loads(result.stdout)
        task.metadata['report'] = report
        task.metadata['grace'] = 1
        task.metadata['status'] = 1
    except subprocess.CalledProcessError as e1:
        task.metadata['grace'] = 1
        task.metadata['status'] = 0
    except subprocess.TimeoutExpired as e2:
        task.metadata['grace'] = 0
        task.metadata['status'] = 0
        print(f"Reporting did not finish on time: {str(e2)}")
    except Exception as e3:
        task.metadata['grace'] = 0
        task.metadata['status'] = 0
        print(f"Reporting subprocess failed due to: {str(e3)}")
    finally:
        task.metadata['timestamp'] = timestamp()
        OrchestratorDone.POST(task.dissolve())
        if task in RUNNING_PROCESSES:
            RUNNING_PROCESSES.remove(task)


@service.post("/submit")
async def submit(task: Task = Depends(Task.build)):
    # Start a new thread for the background task
    threading.Thread(target=start_job, args=(task,)).start()
    OrchestratorRecord.POST_JSON_SAFE(task.withType(f"submit: {task.type}").dissolve())
    return {service.title: "OK"}


@service.get("/health")
def root():
    return {service.title: "Up and Running!"}


@service.get("/status")
def status():
    return {"Running Processes": RUNNING_PROCESSES}


@service.route("/{path:path}")
async def catch_all(path: str):
    raise HTTPException(status_code=404, detail="Not Found")

if __name__ == "__main__":
    try:
        host = OPTS[service.title]['HOST']
        port = OPTS[service.title]['PORT']
        uvicorn.run("src.services.reporter:service",
                    host=host,
                    port=port,
                    reload=False,
                    proxy_headers=True)
    except KeyboardInterrupt:
        print("Stop Signal Recieved!")
