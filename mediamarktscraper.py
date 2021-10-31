import requests
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup as bsp
import json
import os
import pathlib

linkResponse = requests.get(
    "https://www.ikea.com/es/en/p/eftersmak-forced-air-oven-black-70411729/")
soup = bsp(linkResponse.content, 'html.parser')
with open(os.path.dirname(__file__) + os.path.sep + 'ikeaData' + os.path.sep + 'error4.html', 'w') as f:
    f.write(str(soup))
