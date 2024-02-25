from src.utils import *
from src.templates.Payloads import FYREFUSE_PAYLOAD_SCHEMA as PAYLOAD
import sys
import toml

PIPELINES = {
    "airbnb_reviews":       [854],
    "booking_reviews":      [857],
    "priceline_reviews":    [873],
    "hotels_reviews":       [874],
    "booking_listings":     [861],
    "priceline_listings":   [868, 870],
    "hotels_listings":      [869, 871],
    "google_maps":          [856, 860],
    "tripadvisor_listings": [863],
    "tripadvisor_reviews":  [875],
    "airbnb_operational":   [876],
    "airbnb_listings":      [867],
    "flight_schedules":     [865],
}


def extract_logs(logs_dir: Path, source: str):
    ok = logs_dir.exists() and logs_dir.is_dir()
    if not ok:
        raise FileNotFoundError("Provided logs directory either doesn't exist or is file!")
    
    found = {}

    # Search for matching pattern
    for log in logs_dir.iterdir():
        prefix = f"extract_{source}"
        file_name = log.name
        if file_name.startswith(prefix):
            # Create the regular expression pattern using variables
            pattern = rf"{prefix}_{DEFAULT_TIMESTAMP_FORMAT}.log"
            date = to_datetime(log.name, pattern)
            found[date] = log

    if not found:
        raise FileNotFoundError(f"No logs were found for {source} at {logs_dir}")
    
    # Get the most recent datetime key
    most_recent_date = max(found.keys())
    most_recent_logs = found[most_recent_date]
    return most_recent_logs


def insert_log(type, timestamp, kind, message, metadata=None):
    if metadata and metadata.get('message') == message:
        metadata = None
        
    
    PAYLOAD['connector_metadata']['logs'][type].append({
        'timestamp': timestamp,
        'kind': kind,
        'message': message,
        'metadata': metadata,
    })


def initialize_logger(logto: Path, prefix, timestamp, debugging):
    logto = logto.resolve()
    logto.mkdir(parents=True, exist_ok=True)  # ensure path
    log_file_name = f"{logto}/{prefix}_{timestamp}.log"
    init_logger(log_file=log_file_name, debug=debugging)


def parse_arguements():
    parser = argparse.ArgumentParser(description="Reporting")
    parser.add_argument(
        "--logs", '-out',
        type=Path,
        # required=True,
        default=Path('logs/').resolve(),
        help=MSG_ARG_LOGS_OUT,
    )

    parser.add_argument(
        "--log", '-in',
        type=Path,
        # required=True,
        default=None,
        help=MSG_ARG_LOGS_OUT,
    )

    parser.add_argument(
        "--source", '-src',
        type=str,
        required=True,
        help=MSG_ARG_LOGS_OUT,
    )

    parser.add_argument(
        "--provider", '-p',
        type=str,
        required=True,
        help=MSG_ARG_LOGS_OUT,
    )

    parser.add_argument(
        "--status", '-s',
        action="store_true",
        help="Status of Execution",
    )

    parser.add_argument(
        "--grace", '-g',
        action="store_true",
        help="Graceful or Forceful Exit",
    )
    
    parser.add_argument(
        "--debug", '-d',
        action="store_true",
        help="Debug mode",
    )


    return parser.parse_args()

