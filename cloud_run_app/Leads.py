import os
import json
import requests
from datetime import datetime, timedelta
import pendulum
import google.auth.transport.requests
import google.oauth2.id_token as id_token
import logging

URL_CR_SF_BQ = os.environ.get("URL_CR_SF_BQ")
CONN_CR_SF_BQ = os.environ.get("CONN_CR_SF_BQ")
SF_CONN_SECRECT_ID = os.environ.get("SF_CONN_SECRECT_ID")
# generate token for authentication
request = google.auth.transport.requests.Request()
TOKEN = id_token.fetch_id_token(request, URL_CR_SF_BQ)

def handle_response(response,job_name):
    if response.status_code == 200:
        print(f'Salesforce data migration successfully for the job: {job_name}')
        return True
    else:
        print(f'Salesforce data migration failed for the job: {job_name}. Refer to cloud run logs for more details')
        return False

def extract_data():
    headers = {'Authorization': f"Bearer {TOKEN}", "Content-Type": "application/json"}
    data = json.dumps({
            "api_name" : "Lead",
            "raw_dataset" : "SFDC_RAW_DATA",
            "base_table" : "leads",
            "sf_conn": SF_CONN_SECRECT_ID
        })
    response = requests.post(CONN_CR_SF_BQ, headers=headers, data=data, timeout=3600)
    handle_response(response,"sfdc_to_raw_leads")

extract_data()
