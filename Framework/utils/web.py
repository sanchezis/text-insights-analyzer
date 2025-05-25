import requests
from bs4 import BeautifulSoup
import certifi

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        # Send a GET request to the URL
        response = requests.get(url, verify=False, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")

    return soup

def download_file(url, local_filename):
    """
    Download a file from a URL to a local path
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return local_filename

