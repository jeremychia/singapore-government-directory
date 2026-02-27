import requests
from logger import get_logger

logger = get_logger(__name__)


class HTMLDownloader:
    def __init__(self, url):
        self.url = url

    def download_html(self):
        try:
            logger.debug(f"Downloading: {self.url}")
            response = requests.get(self.url, timeout=30)
            if response.status_code == 200:
                logger.debug(f"Successfully downloaded {len(response.content)} bytes")
                return response.content
            else:
                logger.error(
                    f"Failed to retrieve webpage. Status code: {response.status_code}, URL: {self.url}"
                )
                return None
        except requests.exceptions.Timeout:
            logger.error(f"Timeout while downloading: {self.url}")
            return None
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error for {self.url}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {self.url}: {e}")
            return None
