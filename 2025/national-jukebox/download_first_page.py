import json
import pathlib
import requests
import time

import list_urls
import extract_item_info
import extract_mp3


DATA_DIR = pathlib.Path(__file__).parent / "data"


target_url = "https://www.loc.gov/collections/national-jukebox/?sb=date_desc&c=100"
item_urls = list_urls.get_national_jukebox_song_detail_urls(target_url)


def download_and_extract_item(base_url):
    print(f"Fetching content from: {base_url}")
    # https://guides.loc.gov/digital-scholarship/faq
    # Stay within 20 requests per minute rate limit.
    time.sleep(3)
    response = requests.get(base_url)
    while response.status_code == 429:
        print("Too many requests, sleeping")
        time.sleep(10)
        response = requests.get(base_url)

    try:
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return None
    
    item = extract_item_info.extract_subheadings_to_dict(response.text)
    mp3_url = extract_mp3.extract_mp3_url(response.text)
    item["MP3 URL"] = mp3_url
    return item


with open(DATA_DIR / "jukebox.jsonl", "w") as data_file:
    for item_url in item_urls:
        item = download_and_extract_item(item_url)
        json.dump(item, data_file, indent=None)
        data_file.write("\n")
        data_file.flush()
