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

# Authenticate using service account (FIXED VERSION)
def authenticate_drive():
    credentials_path = 'credentials.json'
    gdrive_credentials_json = os.getenv('GDRIVE_CREDENTIALS_JSON')

    if not gdrive_credentials_json:
        raise ValueError("GDRIVE_CREDENTIALS_JSON environment variable not set. Cannot authenticate Google Drive.")

    with open(credentials_path, 'w') as f:
        f.write(gdrive_credentials_json)

    gauth = GoogleAuth()
    gauth.settings['get_refresh_token'] = False
    gauth.LoadServiceConfigFile(credentials_path)
    gauth.ServiceAuth()
    os.remove(credentials_path)  # Clean up
    return GoogleDrive(gauth)

# Upload CSV to Google Drive
def upload_to_drive(local_path, file_name, folder_id):
    try:
        drive = authenticate_drive()
        file = drive.CreateFile({'title': file_name, 'parents': [{'id': folder_id}]})
        file.SetContentFile(local_path)
        file.Upload()
        print(f"✅ Uploaded to Google Drive: {file_name}")
    except Exception as e:
        print(f"❌ Failed to upload {file_name} to Google Drive: {repr(e)}")

# GDrive folder ID (edit access given to service account)
DRIVE_FOLDER_ID = "15gVrByonzFvBMGxJ4NUvVXzCgEUVB1Se"

# Date range
start_date = datetime.today()
end_date = datetime(2017, 1, 1)

# Shopify developer or app URLs
base_urls = [
    'https://apps.shopify.com/partners/litcommerce1',
]

# Fetch apps from developer page
def fetch_shopify_apps(base_url):
    apps = []
    try:
        response = requests.get(base_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch developer page {base_url}: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    divs = soup.select('div.tw-text-body-sm.tw-font-link')

    for div in divs:
        app_name_tag = div.find('a')
        if app_name_tag:
            app_name = app_name_tag.text.strip()
            app_url = app_name_tag['href']
            if not app_url.startswith('http'):
                app_url = f"https://apps.shopify.com{app_url}"
            apps.append({'name': app_name, 'url': app_url})

    print(f"✅ Found {len(apps)} apps on developer page.")
    return apps

# Extract star rating
def extract_rating(review):
    rating_div = review.find('div', class_='tw-flex tw-relative tw-space-x-0.5 tw-w-[88px] tw-h-md')
    if rating_div and 'aria-label' in rating_div.attrs:
        try:
            return rating_div['aria-label'].split(' ')[0]
        except IndexError:
            return None
    return None

# Parse review date
def parse_review_date(date_str):
    if 'Edited' in date_str:
        date_str = date_str.split('Edited')[1].strip()
    try:
        return datetime.strptime(date_str.strip(), '%B %d, %Y')
    except ValueError:
        print(f"⚠️ Could not parse date: '{date_str}'")
        return None

# Fetch reviews for an app
def fetch_reviews(app_url, app_name, start_date, end_date):
    if '/reviews' in app_url:
        base_url = app_url.split('/reviews')[0]
    else:
        base_url = app_url.split('?')[0]

    page = 1
    reviews = []

    retry_strategy = Retry(total=5, backoff_factor=1,
                           status_forcelist=[429, 500, 502, 503, 504],
                           allowed_methods=["HEAD", "GET", "OPTIONS"])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    while True:
        print(f"Fetching page {page} for {app_name}...")
        reviews_url = f"{base_url}/reviews?sort_by=newest&page={page}"

        try:
            response = session.get(reviews_url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed for {reviews_url}: {e}")
            break

        soup = BeautifulSoup(response.content, 'html.parser')
        review_divs = soup.find_all("div", attrs={"data-merchant-review": True},
                                     class_="lg:tw-grid lg:tw-grid-cols-4 lg:tw-gap-x-gutter--desktop")

        if not review_divs:
            print('❌ No more reviews found.')
            break

        has_recent_reviews_on_page = False

        for review_div in review_divs:
            review_text_div = review_div.find('div', {'data-truncate-content-copy': True})
            review_text = review_text_div.find('p').text.strip() if review_text_div and review_text_div.find('p') else "No review text"

            reviewer_name = "No reviewer name"
            location = "N/A"
            duration = "N/A"

            info_block = review_div.find('div', class_='tw-order-2 lg:tw-order-1 lg:tw-row-span-2 tw-mt-md md:tw-mt-0 tw-space-y-1 md:tw-space-y-2 tw-text-fg-tertiary tw-text-body-xs')

            if info_block:
                name_div = info_block.find('div', class_='tw-text-heading-xs tw-text-fg-primary')
                reviewer_name = name_div.text.strip() if name_div else "No reviewer name"
                for child in info_block.find_all('div'):
                    text = child.text.strip()
                    if 'using the app' in text:
                        duration = text.replace(' using the app', '')
                    elif text and text != reviewer_name:
                        location = text

            date_rating = review_div.find('div', class_='tw-flex tw-items-center tw-justify-between tw-mb-md')
            review_date_str = date_rating.find('div').text.strip() if date_rating else "No review date"
            rating = extract_rating(review_div)
            review_date = parse_review_date(review_date_str)

            if review_date:
                if review_date > start_date:
                    has_recent_reviews_on_page = True
                    continue
                elif start_date >= review_date >= end_date:
                    reviews.append({
                        'app_name': app_name,
                        'review': review_text,
                        'reviewer': reviewer_name,
                        'date': review_date_str,
                        'location': location,
                        'duration': duration,
                        'rating': rating
                    })
                    has_recent_reviews_on_page = True
                else:
                    print(f"🛑 Review too old: {review_date_str}")
                    break
            else:
                continue

        if (reviews and review_date and review_date < end_date) or \
           (not has_recent_reviews_on_page and page > 1):
            break

        page += 1
        time.sleep(random.uniform(1.2, 3.0))

    return reviews

# 🚀 Main execution
def main():
    for input_url in base_urls:
        print(f"\n📍 Processing: {input_url}")
        all_reviews = []
        csv_prefix = "shopify_reviews"
        parsed_url = urlparse(input_url)
        segments = [s for s in parsed_url.path.split('/') if s]

        if "/partners/" in input_url:
            print("Detected developer page.")
            handle = segments[-1] if segments else "unknown_dev"
            csv_prefix = f'shopify_developer_reviews_{handle}'
            apps = fetch_shopify_apps(input_url)
            for app in apps:
                reviews = fetch_reviews(app['url'], app['name'], start_date, end_date)
                for r in reviews:
                    r['app_name'] = app['name']
                    all_reviews.append(r)

        elif len(segments) >= 1:
            handle = segments[0] if segments[-1] != 'reviews' else segments[-2]
            if not handle:
                print(f"❌ Could not parse app name: {input_url}")
                continue

            app_name = handle.replace('-', ' ').title()
            base_app_url = f"https://apps.shopify.com/{handle}"
            print(f"🔹 Single app: {app_name}")
            reviews = fetch_reviews(base_app_url, app_name, start_date, end_date)
            for r in reviews:
                r['app_name'] = app_name
                all_reviews.append(r)
            csv_prefix = f'shopify_single_app_reviews_{app_name.replace(" ", "_").lower()}'

        else:
            print(f"❌ Invalid URL: {input_url}")
            continue

        print(f"🔹 Total Reviews Collected: {len(all_reviews)}")

        if all_reviews:
            df = pd.DataFrame(all_reviews)
            now = datetime.now()
            file_name = f'{csv_prefix}_{now.strftime("%Y%m%d_%H%M%S")}.csv'
            df.to_csv(file_name, index=False, encoding='utf-8')
            upload_to_drive(file_name, file_name, DRIVE_FOLDER_ID)
        else:
            print("⚠️ No reviews collected. Skipping file creation.")

# ▶️ Run
if __name__ == '__main__':
    main()
