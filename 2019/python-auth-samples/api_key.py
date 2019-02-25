# Copyright 2019 Google LLC
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

import argparse

import requests


DEFAULT_ADDRESS = "1600 Amphitheatre Parkway, Mountain View, CA"


def main(your_api_key, address=DEFAULT_ADDRESS):
    params = {
        "key": your_api_key,
        "address": address,
    }
    response = requests.get(
        # Enable the Geocoding API to make this request.
        "https://maps.googleapis.com/maps/api/geocode/json",
        params=params)
    print(response.text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("api_key")
    parser.add_argument("--address", default=DEFAULT_ADDRESS)
    args = parser.parse_args()
    main(args.api_key, address=args.address)
