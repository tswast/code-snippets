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

from bs4 import BeautifulSoup
import requests
import json

def extract_subheadings_to_dict(html_content):
    """
    Extracts subheadings from the "About this item" section of HTML
    and returns them as a JSON object.

    Args:
        html_content (str): The HTML content as a string.

    Returns:
        str: A JSON string where each subheading is a key, and its corresponding
             value is a list of items under that subheading.
             Returns an empty JSON object string if the section is not found.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    about_this_item_section = soup.find('div', id='about-this-item')

    if not about_this_item_section:
        return json.dumps({})

    subheadings_data = {}
    
    # Find the div that contains the actual cataloged data
    item_cataloged_data = about_this_item_section.find('div', class_='item-cataloged-data')

    if item_cataloged_data:
        # Iterate through each subheading (h3) within this div
        for h3_tag in item_cataloged_data.find_all('h3'):
            subheading_text = h3_tag.get_text(strip=True)
            items = []
            # The items for each subheading are in the immediately following <ul>
            ul_tag = h3_tag.find_next_sibling('ul')
            if ul_tag:
                for li_tag in ul_tag.find_all('li'):
                    # Get text from list items, handling potential nested structures or links
                    item_text = li_tag.get_text(strip=True)
                    items.append(item_text)
            subheadings_data[subheading_text] = items
            
    # Extract "Part of" section as it's outside item-cataloged-data but still a subheading
    part_of_section = about_this_item_section.find('div', id='part-of')
    if part_of_section:
        h3_tag = part_of_section.find('h3')
        if h3_tag:
            subheading_text = h3_tag.get_text(strip=True)
            items = []
            ul_tag = h3_tag.find_next_sibling('ul')
            if ul_tag:
                for li_tag in ul_tag.find_all('li'):
                    item_text = li_tag.get_text(strip=True)
                    # Remove the count in parentheses if present, e.g., "(10,009)"
                    if '(' in item_text and item_text.endswith(')'):
                        item_text = item_text.rsplit('(', 1)[0].strip()
                    items.append(item_text)
            subheadings_data[subheading_text] = items
            
    # Extract IIIF Presentation Manifest
    iiif_manifest_section = about_this_item_section.find('h3', id='item-iiif-presentation-manifest')
    if iiif_manifest_section:
        subheading_text = iiif_manifest_section.get_text(strip=True)
        items = []
        ul_tag = iiif_manifest_section.find_next_sibling('ul')
        if ul_tag:
            for li_tag in ul_tag.find_all('li'):
                item_text = li_tag.get_text(strip=True)
                items.append(item_text)
        subheadings_data[subheading_text] = items

    return subheadings_data


def download_and_extract(base_url):
    print(f"Fetching content from: {base_url}")
    try:
        response = requests.get(base_url)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return None
    
    item = extract_subheadings_to_dict(response.text)
    item["URL"] = base_url
    return item

# Provided HTML content
if __name__ == "__main__":
    target_url = "https://www.loc.gov/item/jukebox-679643/"
    item = download_and_extract(target_url)
    if item:
        print("\nFound song detail page URLs:")
        print(json.dumps(item, indent=4))
    else:
        print("No song detail URLs found or an error occurred.")
