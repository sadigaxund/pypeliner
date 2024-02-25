from fastapi.middleware.trustedhost import TrustedHostMiddleware
from collections import deque
from datetime import datetime
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Request
from src.templates.Payloads import FYREFUSE_PAYLOAD_SCHEMA 

from pathlib import Path
import subprocess
import threading
import uvicorn
import uuid
import toml
import json
import sys
import os

from src.utils import *

# Define a list of internal IP addresses
internal_ips = ["DPQDALPROD01", "dpqdalprod01", "10.36.1.145",
                    "DPQDALPROD02", "dpqdalprod02", "10.36.1.146",
                    "DPQDALPROD03", "dpqdalprod03", "10.36.1.227",
                    "DPQDALPROD05", "dpqdalprod05", "10.36.1.229",
                    "DPQDALPROD06", "dpqdalprod06", "10.4.2.59",
                    "SER6", "ser6", 'localhost']

CACHE = DatabaseHandler('res/cache.db', table='services', autocommit=True, flag='c')

def debug(message):
    print('\033[93m' + "DEBUG" + '\033[0m' + ":    " + str(message))



if __name__ == "src.services":
    
    with open('res/conf.toml') as f:
        OPTS = toml.loads(f.read())['Services']
        
        #############################################
        # HEALTH CHECKS
        #############################################
        OrchestratorHealth= ApiEndpoint(
            name="Orchestrator",
            base_url=f"http://{OPTS['Orchestrator']['HOST']}:{OPTS['Orchestrator']['PORT']}",
            endpoint='/health',
            interval=2,
            retries=3
        )
        ExtractorHealth = ApiEndpoint(
            name="Extractor",
            base_url=f"http://{OPTS['Extractor']['HOST']}:{OPTS['Extractor']['PORT']}",
            endpoint='/health',
            interval=2,
            retries=3
        )
        ReporterHealth = ApiEndpoint(
            name="Reporter",
            base_url=f"http://{OPTS['Reporter']['HOST']}:{OPTS['Reporter']['PORT']}",
            endpoint='/health',
            interval=2,
            retries=3
        )
        ApifyHealth = ApiEndpoint(
            name="Apify",
            base_url=f"http://{OPTS['Apify']['HOST']}:{OPTS['Apify']['PORT']}",
            endpoint='/health',
            interval=2,
            retries=3
        )
        #############################################
        # ORCHESTRATOR
        #############################################

        OrchestratorSubmit = ApiEndpoint(
            name="Orchestrator",
            base_url=f"http://{OPTS['Orchestrator']['HOST']}:{OPTS['Orchestrator']['PORT']}",
            endpoint='/submit',
            interval=2,
            retries=3
        )

        OrchestratorDone = ApiEndpoint(
            name="Orchestrator",
            base_url=f"http://{OPTS['Orchestrator']['HOST']}:{OPTS['Orchestrator']['PORT']}",
            endpoint='/done',
            interval=2,
            retries=3
        )
        
        OrchestratorRecord = ApiEndpoint(
            name="Orchestrator",
            base_url=f"http://{OPTS['Orchestrator']['HOST']}:{OPTS['Orchestrator']['PORT']}",
            endpoint='/record',
            interval=2,
            retries=3
        )

        #############################################
        # EXTRACTOR
        #############################################

        ExtractorSubmit = ApiEndpoint(
            name="Extractor",
            base_url=f"http://{OPTS['Extractor']['HOST']}:{OPTS['Extractor']['PORT']}",
            endpoint='/submit',
            interval=2,
            retries=3
        )

        #############################################
        # REPORTER
        #############################################

        ReporterSubmit = ApiEndpoint(
            name="Reporter",
            base_url=f"http://{OPTS['Reporter']['HOST']}:{OPTS['Reporter']['PORT']}",
            endpoint='/submit',
            interval=2,
            retries=3
        )

        #############################################
        # FYREFUSE
        #############################################

        

