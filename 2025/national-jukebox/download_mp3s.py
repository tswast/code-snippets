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

import pathlib
import time

import pandas
import requests


DATA_DIR = pathlib.Path(__file__).parent / "data"



def download_mp3(base_url):
    print(f"Fetching content from: {base_url}")
    # https://guides.loc.gov/digital-scholarship/faq
    # Stay within 20 requests per minute rate limit.
    time.sleep(3)

    try:
        response = requests.get(base_url, timeout=60)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return None
    
    return response.content


def download_all():
    jukebox_path = DATA_DIR / "jukebox.jsonl"
    jukebox = pandas.read_json(jukebox_path, lines=True, orient="records")

    # for _, row in jukebox.iterrows():
    for _, row in jukebox.iloc[100:].iterrows():
        jukebox_id = row["URL"].split("/")[-2]
        mp3_path = (DATA_DIR / jukebox_id).with_suffix(".mp3")
        if mp3_path.exists():
            continue

        mp3_bytes = download_mp3(row["MP3 URL"])
        if mp3_bytes is None:
            continue

        with open(mp3_path, "wb") as mp3_file:
            mp3_file.write(mp3_bytes)
        print(f"Wrote {mp3_path}")


if __name__ == "__main__":
    download_all()
