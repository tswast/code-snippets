from bs4 import BeautifulSoup
import requests
import json

def extract_mp3_url(html_content):
    """
    Extracts the MP3 download URL from the given HTML content.

    Args:
        html_content (str): The HTML content of the webpage.

    Returns:
        str or None: The MP3 download URL if found, otherwise None.
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find the select element that contains download options
    # Based on the HTML, it has an ID of 'select-resource0'
    download_select = soup.find('select', id='select-resource0')

    if download_select:
        # Find the option tag specifically for AUDIO download (MP3)
        # It has a data-file-download attribute set to "AUDIO"
        mp3_option = download_select.find('option', attrs={'data-file-download': 'AUDIO'})
        if mp3_option:
            return mp3_option['value'] # Return the value attribute which is the URL
    return None # Return None if the select or option is not found

# Example Usage (assuming you've fetched the HTML using requests)
if __name__ == "__main__":
    url = "https://www.loc.gov/item/jukebox-679643/"
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an exception for HTTP errors
        html_doc = response.text

        mp3_url = extract_mp3_url(html_doc)

        if mp3_url:
            print(f"Extracted MP3 URL: {mp3_url}")
        else:
            print("MP3 URL not found in the HTML.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching the URL: {e}")

