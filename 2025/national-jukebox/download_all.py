# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import pathlib
import time

import pandas
import requests

import list_urls
import extract_item_info
import extract_mp3
import download_mp3s


DATA_DIR = pathlib.Path(__file__).parent / "data"

MAX_PAGES = 169

# https://guides.loc.gov/digital-scholarship/faq
# Stay within 20 requests per minute rate limit.
SLEEP_SECONDS = 60.0 / 20.0

# target_url = "https://www.loc.gov/collections/national-jukebox/?sb=date_desc&c=100"
# target_url_template = "https://www.loc.gov/collections/national-jukebox/?c=100&sb=date_desc&sp={}"
target_url_template = "https://www.loc.gov/collections/national-jukebox/?c=100&sb=date&sp={}"
# target_url_template = "https://www.loc.gov/collections/national-jukebox/?c=100&fa=original-format:sound+recording&sb=date&sp={}"


def download_and_extract_item(base_url):
    print(f"Fetching content from: {base_url}")
    time.sleep(SLEEP_SECONDS)

    try:
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return None
    
    item = extract_item_info.extract_subheadings_to_dict(response.text)
    mp3_url = extract_mp3.extract_mp3_url(response.text)
    item["MP3 URL"] = mp3_url
    item["URL"] = base_url
    return item



def download_page(page_number):
    target_url = target_url_template.format(page_number)
    item_urls = list_urls.get_national_jukebox_song_detail_urls(target_url, sleep_seconds=SLEEP_SECONDS)

    visited_urls = set()
    jukebox_path = DATA_DIR / "jukebox.jsonl"

    if jukebox_path.exists():
        jukebox = pandas.read_json(jukebox_path, lines=True, orient="records")
        visited_urls = frozenset(jukebox["URL"].to_list()) if "URL" in jukebox.columns else {}

    with open(DATA_DIR / "jukebox.jsonl", "a") as data_file:
        while item_urls:
            item_url = item_urls.pop(0)
            if item_url in visited_urls:
                continue

            item = download_and_extract_item(item_url)
            if item is None:
                item_urls.append(item_url)
                continue

            json.dump(item, data_file, indent=None)
            data_file.write("\n")
            data_file.flush()


if __name__ == "__main__":
    page_number = 1
    while True:
        print(f"Page {page_number}")
        try:
            download_page(page_number)
            # Server is currently down for audio.
            # download_mp3s.download_all(sleep_seconds=SLEEP_SECONDS)
        except requests.exceptions.HTTPError as exc:
            if exc.response.status_code == 404:
                print("Reached last page?")
                break
        page_number += 1

        if page_number > MAX_PAGES:
            break

