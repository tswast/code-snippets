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

import google.auth.transport.requests
import pydata_google_auth
import pydata_google_auth.cache
import requests


# Read data from the sheet at
# http://docs.google.com/spreadsheets/d/1i_QCL-7HcSyUZmIbP9E6lO_T5u3HnpLe7dnpHaijg_E/view
#
# Try changing this ID to read data from your own spreadsheet.
SHEET_ID = "1i_QCL-7HcSyUZmIbP9E6lO_T5u3HnpLe7dnpHaijg_E"

credentials = pydata_google_auth.get_user_credentials(
    [
        # userinfo.profile is a minimal scope to get basic account info.
        # In this example, it is used for demonstration of the wrong scopes.
        # This scope does not provide access to Google Sheets data.
        "https://www.googleapis.com/auth/userinfo.profile",

        # Uncomment the spreadsheets scopes to request the correct scopes
        # to access the spreadsheet.
        #"https://www.googleapis.com/auth/spreadsheets"
    ],

    # Request credentials every time. Do not cache them to disk.
    credentials_cache=pydata_google_auth.cache.NOOP,

    # Use a local webserver so that the user doesn't have to copy-paste an
    # authentication code.
    auth_local_webserver=True,
)

# Exchange a refresh token for a temporary access token.
request = google.auth.transport.requests.Request()
credentials.refresh(request)

# Attach the access token to the request to the Google Sheets API via an HTTP
# "Authorization" header.
headers = {
    "Authorization": "Bearer {}".format(credentials.token),
}
response = requests.get(
    "https://sheets.googleapis.com/v4/spreadsheets/{}".format(SHEET_ID)
    headers=headers,
)
print(response)
print(response.text)

