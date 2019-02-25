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

import google.auth
import google.auth.transport.requests
import requests


credentials, _ = google.auth.default([
    "https://www.googleapis.com/auth/cloud-platform",
])

# Exchange for a temporary access token.
request = google.auth.transport.requests.Request()
credentials.refresh(request)

# Attach the access token to the request to the Cloud Storage JSON API via an
# HTTP "Authorization" header.
headers = {
    "Authorization": "Bearer {}".format(credentials.token),
}
response = requests.get(
    "https://www.googleapis.com/storage/v1/b/bucket-you-cant-access/o/README.txt",
    headers=headers,
)
print(response)
print(response.text)

