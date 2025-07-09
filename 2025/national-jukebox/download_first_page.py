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


DATA_DIR = pathlib.Path(__file__).parent / "data"


target_url = "https://www.loc.gov/collections/national-jukebox/?sb=date_desc&c=100"
item_urls = list_urls.get_national_jukebox_song_detail_urls(target_url)


def download_and_extract_item(base_url):
    print(f"Fetching content from: {base_url}")
    # https://guides.loc.gov/digital-scholarship/faq
    # Stay within 20 requests per minute rate limit.
    time.sleep(3)
    response = requests.get(base_url)

    try:
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return None
    
    item = extract_item_info.extract_subheadings_to_dict(response.text)
    mp3_url = extract_mp3.extract_mp3_url(response.text)
    item["MP3 URL"] = mp3_url
    item["URL"] = base_url
    return item


visited_urls = {}
jukebox_path = DATA_DIR / "jukebox.jsonl"

if jukebox_path.exists():
    jukebox = pandas.read_json(jukebox_path, lines=True, orient="records")
    visited_urls = frozenset(jukebox["URL"].to_list()) if "URL" in jukebox.columns else {}


with open(DATA_DIR / "jukebox.jsonl", "a") as data_file:
    for item_url in item_urls:
        if item_url in visited_urls:
            continue

        item = download_and_extract_item(item_url)
        if item is None:
            continue

        json.dump(item, data_file, indent=None)
        data_file.write("\n")
        data_file.flush()
