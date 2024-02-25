from sympy import EX
from src.report import *

###############################################
# HELPER FUNCTIONS
###############################################

def unflag():
    PAYLOAD['flag'] = False

def disgrace():
    PAYLOAD['connector_metadata']['grace'] = False

def extract_namespace_metadata(meta: Dict):
    if meta.get('Namespace'):
        PAYLOAD['connector_metadata']['summary']['pipeline_key_timestamp'] = meta['Namespace'].get('TIMESTAMP')
        PAYLOAD['connector_metadata']['summary']['recurrency_start_date'] = meta['Namespace'].get('EXTRACT_START_DATE')
        PAYLOAD['connector_metadata']['summary']['recurrency_stop_date'] = meta['Namespace'].get('EXTRACT_END_DATE')
        return True
    else:
        return False

def extract_kafka_metadata(meta: Dict):
    if meta.get('Kafka Metadata'):
        PAYLOAD['connector_metadata']['batch_key'] = meta['Kafka Metadata']['batch_key']
        PAYLOAD['connector_metadata']['kafka_topic'] = meta['Kafka Metadata']['topic_name']
        return True
    else:
        return False


metadata_extractors = [
    extract_kafka_metadata, 
    extract_namespace_metadata
]

###############################################
# MAIN FUNCTIONS
###############################################

@trace(message="Processing Log File")
def process_log(log_file: Path):
    
    with open(log_file, 'r') as log_file:
        lines = log_file.readlines()

        for i, line in enumerate(lines):
            line = line.strip()
            comps = line.split(DEFAULT_LOG_SEPERATOR)
            # Parse information
            timestamp = int(comps[0])
            
            # scrape timestamps
            if i == 0:
                PAYLOAD['connector_metadata']['summary']['pipeline_start_timestamp'] = timestamp
            if i == len(lines) - 1:
                PAYLOAD['connector_metadata']['summary']['pipeline_complete_timestamp'] = timestamp

            # ignore [1,2]
            
            level = comps[3]

            message = comps[4]

            metadata = (comps[-1])
            metadata = safe_convert_to_json(metadata)

            for scraper in metadata_extractors:
                try:
                    if scraper(metadata):
                        metadata_extractors.remove(scraper)
                        break
                except Exception as e:
                    metadata_extractors.remove(scraper)

            if level == 'TRACE':
                if message == 'STATISTICS':
                    insert_log('progress', timestamp,
                                'system', message, metadata)
                    continue
                elif message in ["STARTED", "FINISHED"]:
                    message += " " + comps[5]
                    insert_log('infos', timestamp,
                                'system', message)
                    continue
            elif level == 'INFO':
                insert_log('infos', timestamp, 'source', message, metadata)
                continue
            elif level == 'DEBUG':
               continue  # ignore all debug
            elif level == 'ERROR':
                if message in ['EXTRACT', 'TRANSFORM', 'LOAD']:
                    insert_log('errors', timestamp,
                                'source', message, metadata)
                    continue
                else:
                    insert_log('errors', timestamp,
                                'system', message, metadata)
                    continue
            elif level == 'WARNING':
                insert_log('warnings', timestamp, 'system', message, metadata)
                continue

@trace(message="Counting Staged Records from Kafka")
@handle(error_message="Could not count Staged Records from Kafka",
        strict=False,
        do_raise=False,
        max_retries=3,
        timeout_secs=3,    
        on_error=disgrace,
        fallback_value= (-1, -1))
def count_staged_records(kafka_topic, batch_key, timeout=5):
    printlog("Kafka Metadata",'info', {"kafka_topic": kafka_topic, "batch_key": batch_key})
    if not kafka_topic or not batch_key:
        raise ValueError('Either kafka topic or batch key is null.')
    
    with open('res/conf.toml') as cnf:
        confs = toml.loads(cnf.read())
        KAFKA_BROKER = confs['Kafka']['BROKER']
    
    records = 0
    size = 0
    
    with get_or_create_kafka_consumer(broker_url=KAFKA_BROKER,
                                      topic_name=kafka_topic) as consumer:
        while True:
            msg = consumer.poll(timeout=timeout)

            if msg is None:
                printlog(f"Timeout ({timeout} ms) reached. No more records left to consume.", 'info')
                break

            if msg.error():
                printlog(f"Consumer error: {msg.error()}", "error")
                continue

            # Skip Unnecessary Records
            if batch_key != default_key_deserializer(msg.key()):
                continue
            
            records += 1
            size += len(msg.value())
        return records, size


@trace("Generating Report")
@handle(error_message="Unexpected Error while generating report",
        on_error=disgrace,
        do_raise=False,
        strict=True)
def generate_report(
    status,
    grace,
    source,
    provider,
    pipelines_id,
    log_path,
    log_file,
):
    # ADD HEADERS
    PAYLOAD['flag'] = status
    PAYLOAD['connector_metadata']['grace'] = grace
    PAYLOAD['connector_metadata']['data_provider'] = provider
    PAYLOAD['pipelines_id'] = pipelines_id
    
    # PARSE LOG FILE
    log_to_process = None
    if log_file and log_file.exists() and log_file.is_file():
        # then we all good, just process it
        log_to_process = log_file
    else:
        # couldn't load exact log, try to find it
        printlog(f"Passed Bad log file", 'warning', {'log': log_file})
        printlog(f"Searching for latest unprocessed log file...", 'info')
        try:
            log_to_process = extract_logs(logs_dir=log_path, source=source)
            printlog(f"Found log file", 'info', {'log': log_to_process})
        except FileNotFoundError as fme:
            printlog(f"Could not find any logs", 'error', {'reason': fme})
            disgrace()
            unflag()
            return
    
    # PROCESS IF LOG FILE IS GOOD     
    process_log(log_to_process)
    
    # COUNT STAGED RECORDS
    records, size = count_staged_records(
        batch_key=PAYLOAD['connector_metadata']['batch_key'],
        kafka_topic=PAYLOAD['connector_metadata']['kafka_topic'])

    PAYLOAD['connector_metadata']['summary']['staged_records'] = records
    PAYLOAD['connector_metadata']['summary']['staged_size'] = size
    
    ####################################
    # ADD COMPLETION
    PAYLOAD['connector_metadata']['summary']['completion'] = PAYLOAD['connector_metadata']['logs']['progress'][-1]['metadata']['Extra']['Completion']
    PAYLOAD['connector_metadata']['summary']['extracted_size'] = PAYLOAD['connector_metadata']['logs']['progress'][-1]['metadata']['Extra']['Size']
    
    # ADD DURATION
    timestamp1 = PAYLOAD['connector_metadata']['summary']['pipeline_start_timestamp']
    timestamp2 = PAYLOAD['connector_metadata']['summary']['pipeline_complete_timestamp']
    # Convert timestamps to datetime objects
    datetime1 = datetime.utcfromtimestamp(timestamp1 / 1000.0)
    datetime2 = datetime.utcfromtimestamp(timestamp2 / 1000.0)
    # Calculate duration in seconds
    duration_seconds = (datetime2 - datetime1).total_seconds()

    PAYLOAD['connector_metadata']['summary']['duration'] = duration_seconds
    PAYLOAD['connector_metadata']['summary']['extracted_records'] = PAYLOAD[
        'connector_metadata']['logs']['progress'][-1]['metadata']['Success']['Extract']
    
    ## REDUCE PROGRESS REPORT
    old_progress = PAYLOAD['connector_metadata']['logs']['progress']
    seen = set()
    new_progress = []
    # Deduplicate progress report
    for d in old_progress[:-1]:
        completion = d['metadata']['Extra']['Completion']
        if completion not in seen:
            seen.add(completion)
            new_progress.append(d)

    # Reduce sample size of progress
    TOTAL_STEPS = 10
    STEP_SIZE = max(len(new_progress) // TOTAL_STEPS, 1)
    PAYLOAD['connector_metadata']['logs']['progress'] = new_progress[::STEP_SIZE]
    PAYLOAD['connector_metadata']['logs']['progress'].append(old_progress[-1])
    
    # MOVE LOG FILE TO ARCHIVE
    new_directory = log_to_process.parent / "archive"
    new_directory.mkdir(parents=True, exist_ok=True)
    # Construct the new path within the 'archive' directory
    new_path = new_directory / log_to_process.name
    # Move the file
    log_to_process.rename(new_path)

def save_and_send_report():
    REPORT_PATH = Path(f'{ARGS.logs}/reports/{ARGS.provider}_{ARGS.source}-{timestamp()}.report').resolve()
    printlog('Report Done', 'info', {"Generated Report": str(REPORT_PATH)})
    report = json.dumps(PAYLOAD, ensure_ascii=False, default=custom_encoder)
    # Log to file
    with open(REPORT_PATH, 'w') as f:
        f.write(report)
        
    # Output to Std
    sys.stdout.write(report)

if __name__ == '__main__':
    ARGS = parse_arguements()
    
    try:
        initialize_logger(logto=ARGS.logs,
                      prefix='report_' + ARGS.source,
                      timestamp=timestamp(),
                      debugging=ARGS.debug)
        generate_report(log_path=ARGS.logs,
                        log_file=ARGS.log,
                        status=ARGS.status,
                        grace=ARGS.grace,
                        source=ARGS.source,
                        provider=ARGS.provider,
                        pipelines_id=PIPELINES[ARGS.source],)
    except:
        unflag()
    save_and_send_report()
    
    
    
    
    
