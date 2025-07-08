import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def get_national_jukebox_song_detail_urls(base_url: str) -> list[str]:
    """
    Scrapes the National Jukebox collection page to extract URLs for individual song detail pages.

    Args:
        base_url: The URL of the main collection page (e.g., "https://www.loc.gov/collections/national-jukebox/?sb=date_desc").

    Returns:
        A list of URLs for the song detail pages.
    """
    print(f"Fetching content from: {base_url}")
    try:
        response = requests.get(base_url)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    detail_urls = []

    # The structure of the page suggests that song links are typically within
    # elements that represent individual items. Looking for common patterns like 'div'
    # with specific classes or 'a' tags directly.
    # From a quick inspection, it seems 'a' tags with hrefs pointing to individual
    # records are nested within list items or similar structures.
    # Let's try to find all links within a common container for search results.

    # Assuming the main container for search results items has a class like 'item-results'
    # or similar, and individual items have links within them.
    # We'll look for <a> tags whose href attributes match a pattern for detail pages.
    # A common pattern for Library of Congress detail pages is /item/{id}/ or /record/{id}/
    # Let's target links that contain '/item/' in their href and are likely part of the main results.

    # Find all 'a' tags that have an 'href' attribute
    for link in soup.find_all('a', href=True):
        href = link['href']
        # Check if the href points to a detail page.
        # Examples: /item/jukebox-12345/ or similar.
        # We need to construct absolute URLs.
        if '/item/jukebox' in href and not href.startswith('#'):
            full_url = urljoin(base_url, href)
            # Avoid adding duplicates if the same item link appears multiple times
            if full_url not in detail_urls:
                detail_urls.append(full_url)

    return detail_urls

if __name__ == "__main__":
    target_url = "https://www.loc.gov/collections/national-jukebox/?sb=date_desc&c=100"
    song_urls = get_national_jukebox_song_detail_urls(target_url)

    if song_urls:
        print("\nFound song detail page URLs:")
        for url in song_urls:
            print(url)
        print(f"\nTotal URLs found: {len(song_urls)}")
    else:
        print("No song detail URLs found or an error occurred.")
