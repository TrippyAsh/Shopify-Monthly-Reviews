
import requests
from bs4 import BeautifulSoup
from bs4 import Tag
import pandas as pd
from datetime import datetime
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse
import os
import json
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# Authenticate using service account
def authenticate_drive():
    credentials_path = 'credentials.json'
    with open(credentials_path, 'w') as f:
        f.write(os.getenv('GDRIVE_CREDENTIALS_JSON'))
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(credentials_path)
    gauth.ServiceAuth()
    return GoogleDrive(gauth)

# Upload CSV to a specific Google Drive folder
def upload_to_drive(local_path, file_name, folder_id):
    drive = authenticate_drive()
    file = drive.CreateFile({'title': file_name, 'parents': [{'id': folder_id}]})
    file.SetContentFile(local_path)
    file.Upload()
    print(f"✅ Uploaded to Google Drive: {file_name}")

# REPLACE this with your actual folder ID
DRIVE_FOLDER_ID = "PASTE_YOUR_DRIVE_FOLDER_ID_HERE"

# Your original script continues here...
