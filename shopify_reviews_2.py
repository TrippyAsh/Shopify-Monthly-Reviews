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
    # Check if the environment variable is set
    gdrive_credentials_json = os.getenv('GDRIVE_CREDENTIALS_JSON')
    if not gdrive_credentials_json:
        raise ValueError("GDRIVE_CREDENTIALS_JSON environment variable not set. Cannot authenticate Google Drive.")

    with open(credentials_path, 'w') as f:
        f.write(gdrive_credentials_json)

    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(credentials_path)
    gauth.ServiceAuth()
    return GoogleDrive(gauth)

# Upload CSV to a specific Google Drive folder
def upload_to_drive(local_path, file_name, folder_id):
    try:
        drive = authenticate_drive()
        file = drive.CreateFile({'title': file_name, 'parents': [{'id': folder_id}]})
        file.SetContentFile(local_path)
        file.Upload()
        print(f"✅ Uploaded to Google Drive: {file_name}")
    except Exception as e:
        print(f"❌ Failed to upload {file_name} to Google Drive: {e}")
    finally:
        # Clean up the credentials file after use
        if os.path.exists('credentials.json'):
            os.remove('credentials.json')

# REPLACE this with your actual folder ID
DRIVE_FOLDER_ID = "15gVrByonzFvBMGxJ4NUvVXzCgEUVB1Se"

"""
# Combined logic to scrape Single app review page or the Developer page and save the data to  Google Drive.
"""

# 📅 Date Configuration
start_date = datetime.today() # Collects reviews up to today
end_date = datetime(2017, 1, 1) # Collects reviews from this date onwards (inclusive)

# 🌐 Shopify URLs (Feel free to add more - can be developer pages or single app pages)
base_urls = [
    'https://apps.shopify.com/partners/cedcommerce',
    'https://apps.shopify.com/partners/tanishqandmac',
    'https://apps.shopify.com/partners/digital-product-labs',
    'https://apps.shopify.com/partners/etsify-io',
    'https://apps.shopify.com/partners/common-services',
    'https://apps.shopify.com/partners/ecom-planners2',
    'https://apps.shopify.com/partners/litcommerce1',
    'https://apps.shopify.com/marketplace-connect/reviews'
]

# 🧠 Function: Fetch Apps from Developer Page
def fetch_shopify_apps(base_url):
    """
    Fetches a list of all Shopify apps associated with a given developer page.

    Args:
        base_url (str): The base URL of the Shopify developer's app page.

    Returns:
        list: A list of dictionaries, where each dictionary contains the 'name'
              and 'url' of an app.
    """
    apps = []
    try:
        response = requests.get(base_url)
        response.raise_for_status() # Raise an exception for bad status codes
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch developer page {base_url}: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')

    # Select all div elements that contain the app name and link.
    divs = soup.select('div.tw-text-body-sm.tw-font-link')

    for div in divs:
        app_name_tag = div.find('a')
        if app_name_tag:
            app_name = app_name_tag.text.strip()
            app_url = app_name_tag['href']

            # Ensure the app URL is absolute.
            if not app_url.startswith('http'):
                app_url = f"https://apps.shopify.com{app_url}"
            apps.append({'name': app_name, 'url': app_url})

    print(f"✅ Found {len(apps)} apps on developer page.")
    return apps

# 🧠 Function: Extract Rating
def extract_rating(review):
    """
    Extracts the star rating from a given review's BeautifulSoup object.

    Args:
        review (bs4.Tag): A BeautifulSoup Tag object representing a single review block.

    Returns:
        str or None: The star rating (e.g., "5") if found, otherwise None.
    """
    # Find the div containing the aria-label with the rating information.
    rating_div = review.find('div', class_='tw-flex tw-relative tw-space-x-0.5 tw-w-[88px] tw-h-md')
    if rating_div and 'aria-label' in rating_div.attrs:
        aria_label = rating_div['aria-label']
        try:
            # The rating is typically the first part of the aria-label (e.g., "5 out of 5 stars").
            return aria_label.split(' ')[0]
        except IndexError:
            return None
    return None

# 🧠 Function: Parse Review Date
def parse_review_date(date_str):
    """
    Converts a Shopify review date string into a Python datetime object.

    Handles cases where the date string might include "Edited".

    Args:
        date_str (str): The date string from the review (e.g., "June 1, 2024" or "Edited June 1, 2024").

    Returns:
        datetime or None: A datetime object if parsing is successful, otherwise None.
    """
    if 'Edited' in date_str:
        date_str = date_str.split('Edited')[1].strip()
    else:
        date_str = date_str.strip()
    try:
        return datetime.strptime(date_str, '%B %d, %Y')
    except ValueError:
        print(f"⚠️ Could not parse date string: '{date_str}'. Returning None.")
        return None

# 🧠 Function: Fetch Reviews
def fetch_reviews(app_url, app_name, start_date, end_date):
    """
    Fetches all reviews for a specific Shopify app within a given date range.

    Reviews are fetched page by page, sorted by newest, until an old review
    (outside the start_date and end_date range) is encountered. Includes
    a retry mechanism for robustness.

    Args:
        app_url (str): The URL of the Shopify app's page (e.g., https://apps.shopify.com/app-name).
        app_name (str): The name of the Shopify app.
        start_date (datetime): The inclusive start date for review collection.
        end_date (datetime): The inclusive end date for review collection.

    Returns:
        list: A list of dictionaries, where each dictionary represents a review
              with details like text, reviewer, date, rating, location, and duration.
    """
    # Ensure the URL is clean and points to the app's base page, not directly to reviews.
    # If the URL already contains '/reviews', remove it for constructing the base URL.
    if '/reviews' in app_url:
        base_url = app_url.split('/reviews')[0]
    else:
        base_url = app_url.split('?')[0] # Clean any query parameters if present

    page = 1
    reviews = []

    # Configure retries for the requests session
    retry_strategy = Retry(
        total=5,  # Total number of retries
        backoff_factor=1,  # Factor by which delay increases (1, 2, 4, 8, 16 seconds)
        status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
        allowed_methods=["HEAD", "GET", "OPTIONS"]  # HTTP methods to retry
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    while True:
        print(f"Fetching page {page} for {app_name}...")
        # Construct the reviews URL correctly, ensuring it's always '/reviews?sort_by=newest&page=X'
        reviews_url = f"{base_url}/reviews?sort_by=newest&page={page}"

        try:
            response = session.get(reviews_url)
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed for {reviews_url}: {e}")
            # If the request fails after retries, stop fetching for this app.
            break

        soup = BeautifulSoup(response.content, 'html.parser')

        # Select all review blocks based on their attributes and class.
        review_divs = soup.find_all("div", attrs={"data-merchant-review": True},
                                     class_="lg:tw-grid lg:tw-grid-cols-4 lg:tw-gap-x-gutter--desktop")

        print(f"🔹 Found {len(review_divs)} reviews on page {page}")

        if not review_divs:
            print('❌ No more reviews found. Stopping.')
            break

        has_recent_reviews_on_page = False

        for review_div in review_divs:
            # Extract review text.
            review_text_div = review_div.find('div', {'data-truncate-content-copy': True})
            review_text = review_text_div.find('p').text.strip() if review_text_div and review_text_div.find('p') else "No review text"

            reviewer_name = "No reviewer name"
            location = "N/A"
            duration = "N/A"

            # Locate the reviewer information block.
            reviewer_info_block = review_div.find('div', class_='tw-order-2 lg:tw-order-1 lg:tw-row-span-2 tw-mt-md md:tw-mt-0 tw-space-y-1 md:tw-space-y-2 tw-text-fg-tertiary tw-text-body-xs')

            if reviewer_info_block:
                # Extract reviewer name.
                reviewer_name_div = reviewer_info_block.find('div', class_='tw-text-heading-xs tw-text-fg-primary tw-overflow-hidden tw-text-ellipsis tw-whitespace-nowrap')
                reviewer_name = reviewer_name_div.text.strip() if reviewer_name_div else "No reviewer name"

                # Extract location and duration by iterating through child divs.
                found_location = False
                info_children_divs = [child for child in reviewer_info_block.children if isinstance(child, Tag) and child.name == 'div']

                for child_div in info_children_divs:
                    if child_div == reviewer_name_div: # Skip the name div itself.
                        continue

                    text_content = child_div.text.strip()
                    if 'using the app' in text_content: # Identify duration by a specific phrase.
                        duration = text_content.replace(' using the app', '')
                    elif not found_location and len(text_content) > 0: # Assign first non-empty div to location.
                        location = text_content
                        found_location = True

            # Extract review date.
            date_and_rating_container = review_div.find('div', class_='tw-flex tw-items-center tw-justify-between tw-mb-md')
            review_date_str = "No review date"
            if date_and_rating_container:
                review_date_div = date_and_rating_container.find('div', class_='tw-text-body-xs tw-text-fg-tertiary')
                review_date_str = review_date_div.text.strip() if review_date_div else "No review date"

            # Extract rating using the helper function.
            rating = extract_rating(review_div)

            # Parse the date string into a datetime object for comparison.
            review_date = parse_review_date(review_date_str)

            if review_date:
                # Check if the review date is too new (after start_date).
                # If so, skip it and continue to the next review on the page.
                if review_date > start_date:
                    has_recent_reviews_on_page = True
                    continue
                # Check if the review date is within the desired range.
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
                    # If the review is older than the end_date, stop fetching for this app.
                    print(f"🛑 Review too old: {review_date_str}. Stopping for {app_name}.")
                    break # Break out of the inner for loop (reviews on current page)
            else:
                # parse_review_date already prints a warning. Continue to next review.
                continue

        # Logic to determine if we should stop fetching pages.
        # If the inner loop broke because a review was too old, or no relevant reviews were found
        # on the current page (and it's not the first page), stop.
        if (reviews and review_date and review_date < end_date) or \
           (not has_recent_reviews_on_page and page > 1):
            print(f'✅ All relevant reviews collected for {app_name}, or no new reviews found in the date range on this page.')
            break

        page += 1
        # Introduce a random delay to avoid overwhelming the server.
        time.sleep(random.uniform(1.2, 3.0))

    return reviews

# 🚀 Main Execution
def main():
    """
    Main function to orchestrate fetching app details and their reviews,
    then saving the collected data to CSV files, based on the URL type.
    """
    for input_url in base_urls:
        print(f"\n📍 Processing URL: {input_url}")
        all_collected_reviews_for_url = []
        csv_filename_prefix = "shopify_reviews"

        parsed_url = urlparse(input_url)
        path_segments = [s for s in parsed_url.path.split('/') if s]

        if "/partners/" in input_url:
            print("Detected developer page URL.")
            # Extract developer handle for CSV naming
            developer_handle = path_segments[-1] if path_segments else "unknown_developer"
            csv_filename_prefix = f'shopify_developer_reviews_{developer_handle}'

            # Fetch all apps from the specified developer page.
            apps = fetch_shopify_apps(input_url)
            print(f"🔹 Total Apps Found: {len(apps)}")

            # Iterate through each app and fetch its reviews within the defined date range.
            for app in apps:
                reviews = fetch_reviews(app['url'], app['name'], start_date, end_date)
                for review in reviews:
                    # Ensure app_name is explicitly added to each review dictionary
                    review['app_name'] = app['name']
                    all_collected_reviews_for_url.append(review)

        elif len(path_segments) >= 1 and (len(path_segments) == 1 or (len(path_segments) >= 2 and path_segments[-1] == 'reviews')):
            print("Detected single app URL.")
            # Determine app handle: if last segment is 'reviews', then the handle is the one before it.
            # Otherwise, it's the first (and only) segment.
            app_handle = path_segments[0] if path_segments[-1] != 'reviews' else path_segments[-2]

            if not app_handle:
                print(f"❌ Could not parse app name from URL: {input_url}. Skipping.")
                continue

            base_app_url = f"https://apps.shopify.com/{app_handle}"
            app_name = app_handle.replace('-', ' ').title()

            print(f"🔹 Fetching reviews for single app: {app_name} ({base_app_url})")

            reviews = fetch_reviews(base_app_url, app_name, start_date, end_date)

            for review in reviews:
                # Ensure app_name is explicitly added to each review dictionary
                review['app_name'] = app_name
                all_collected_reviews_for_url.append(review)

            csv_filename_prefix = f'shopify_single_app_reviews_{app_name.replace(" ", "_").lower()}'

        else:
            print(f"❌ Invalid Shopify URL provided: {input_url}. Skipping.")
            continue

        print(f"🔹 Total Reviews Collected for {input_url}: {len(all_collected_reviews_for_url)}")

        if all_collected_reviews_for_url:
            # Create a Pandas DataFrame from the collected data.
            df = pd.DataFrame(all_collected_reviews_for_url)

            # Generate a timestamped filename for the CSV output.
            now = datetime.now()
            file_name = f'{csv_filename_prefix}_{now.strftime("%Y%m%d_%H%M%S")}.csv'

            # Save the DataFrame to a CSV file and upload to Google Drive
            df.to_csv(file_name, index=False, encoding='utf-8')
            upload_to_drive(file_name, file_name, DRIVE_FOLDER_ID)

            print(f"✅ Data saved to: {file_name}")
        else:
            print(f"⚠️ No reviews were collected for {input_url}. CSV file not created.")

# ▶️ Run the script
if __name__ == '__main__':
    main()
