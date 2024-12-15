import requests


class HTMLDownloader:
    def __init__(self, url):
        self.url = url

    def download_html(self):
        try:
            response = requests.get(self.url)
            if response.status_code == 200:
                return response.content
            else:
                print(
                    "Failed to retrieve the webpage. Status code:", response.status_code
                )
                return None
        except requests.exceptions.RequestException as e:
            print("An error occurred:", e)
            return None
