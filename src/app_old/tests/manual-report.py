from pathlib import Path
import requests
import sys

# Get command-line arguments
args = sys.argv

REPORTS_PATH = Path(args[1]).resolve()

reports = REPORTS_PATH.iterdir()


for report in reports:
    with open(report, 'r') as f:
        contents = f.read()
        requests.post("DPQDALPROD02:8000/fusilli/deployer/instance/sequential/starter/", data=contents)
