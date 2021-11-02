import requests
from bs4 import BeautifulSoup as bsp
import json
from requests.exceptions import HTTPError
import os
import re
import pathlib
from concurrent.futures import ThreadPoolExecutor
from itertools import zip_longest


# ---------------------- Links related functions --------------------------------------------

maxApiConcurrentCalls = 200


def getFullListOfProducts():
    baseUrl = "https://canarias.mediamarkt.es/"
    try:
        response = requests.get(
            "https://canarias.mediamarkt.es/sitemap_collections_1.xml")
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    soup = bsp(response.content, 'xml')

    allCategoriesNames = list(
        map(lambda x: x.text.split(os.path.sep)[-1], soup.find_all('loc')))
    uniqueProductsLinks = set()
    for catSlice in grouper(allCategoriesNames, maxApiConcurrentCalls, None):
        catSlice = list(filter(lambda x: x is not None, catSlice))
        try:
            with ThreadPoolExecutor(max_workers=maxApiConcurrentCalls) as pool:
                responses = list(
                    pool.map(getProductsLinksResponseJSON, catSlice))
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')
        for i in responses:
            productsLinks = list(
                map(lambda x: baseUrl + 'products' + os.path.sep + x["link"].split('products' + os.path.sep)[1], i["items"]))
            uniqueProductsLinks.update(productsLinks)
    pathlib.Path(os.path.dirname(__file__) + os.path.sep +
                 'mediamarktData').mkdir(parents=True, exist_ok=True)
    updateLinks(uniqueProductsLinks)


def updateLinks(links):
    fullCatalogLinks = os.path.dirname(
        __file__) + os.path.sep + 'mediamarktData' + os.path.sep + 'links.txt'
    with open(fullCatalogLinks, 'w') as file:
        for s in links:
            file.write(s + '\n')


def fetchLinks():
    with open(os.path.dirname(__file__) + os.path.sep + 'mediamarktData' + os.path.sep + 'links.txt', 'r') as f:
        productsLinks = [line.rstrip('\n') for line in f]
    return productsLinks


def getUrl(url):
    return requests.get(url)


def getProductsLinksResponseJSON(sectionName):
    return requests.get("https://www.searchanise.com/getresults?api_key=1W7C4E0H3O&sortBy=sales_amount&sortOrder=desc&startIndex=0&maxResults=5000&items=true&pages=true&categories=true&queryCorrection=true&pageStartIndex=0&pagesMaxResults=5000&categoryStartIndex=0&categoriesMaxResults=20&facets=true&facetsShowUnavailableOptions=false&ResultsTitleStrings=3&collection=" + sectionName + "&output=json").json()


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


try:
    productsLinks = fetchLinks()
except:
    print("Creating a new list of all the links in the ikea page.")
    getFullListOfProducts()
    productsLinks = fetchLinks()
    print("Fetched all links.")

# --------------------- Mappping functions ---------------------------------


def productToPacket(packageData, characteristics):
    packet = {}
    packet["id"] = ""
    packet["name"] = packageData["title"][:40]
    packet["description"] = packageData["type"]
    packet["productId"] = packageData["id"]
    packet["subgroupId"] = packageData["id"]
    packet["rounded"] = 0
    characteristics = list(map(lambda x: x.find(
        'td', class_='spec-line-value').text, characteristics))
    packet["width"], packet["height"], packet["length"] = float(characteristics[3].split(
        ' ')[0]), float(characteristics[2].split(' ')[0]), float(characteristics[1].split(' ')[0])
    packet["weight"] = float(characteristics[0].split(' ')[0]) if characteristics[0].split(
        ' ')[1] == 'kg' else float(characteristics[0].split(' ')[0])/1000
    if packet["width"] or packet["height"] or packet["length"]:
        return packet
    else:
        return None


def productBuilder(linkResponse):
    soup = bsp(linkResponse.content, 'html.parser')
    try:
        productJSON = json.loads(soup.find('script', text=re.compile(
            'MRParams')).text.split('"total_quantity" : "0",')[1].strip().split('"product" : ')[1][:-1])
        characteristics = list(filter(lambda x: any(a in x.text for a in [
            "del embalaje", "embalado"]), soup.find_all('tr')))
        if len(characteristics) == 4:
            return productToPacket(productJSON, characteristics)
        else:
            return None
    except:
        return None


# -------------- Main --------------------------------------
mainPacketsList = []
dataFilename = os.path.dirname(
    __file__) + os.path.sep + 'mediamarktData' + os.path.sep + 'data.json'
backupFilename = os.path.dirname(
    __file__) + os.path.sep + 'mediamarktData' + os.path.sep + 'from.json'


def get_url(url):
    return requests.get(url)


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


maxApiConcurrentCalls = 50


def chargeCurrentMaxLinkIndex():
    try:
        with open(os.path.dirname(__file__) + os.path.sep + 'mediamarktData' + os.path.sep + 'from.json', 'r') as f:
            currentMaxLinkIndex = productsLinks.index(
                json.load(f)[0]["link"]) + 1
        return currentMaxLinkIndex
    except:
        return 0


currentStartingLinkIndex = chargeCurrentMaxLinkIndex()
if currentStartingLinkIndex:
    with open(os.path.dirname(__file__) + os.path.sep + 'mediamarktData' + os.path.sep + 'data.json', 'r') as f:
        mainPacketsList = json.load(f)

for linksSlice in grouper(productsLinks[currentStartingLinkIndex:], maxApiConcurrentCalls, None):
    linksSlice = list(filter(lambda x: x is not None, linksSlice))
    responses = []
    try:
        with ThreadPoolExecutor(max_workers=maxApiConcurrentCalls) as pool:
            responses = list(pool.map(get_url, linksSlice))
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')

    wrongResponseLinks = list(
        filter(lambda x: x not in linksSlice, list(map(lambda y: y.url, responses))))
    actualProductLinks = productsLinks
    if len(wrongResponseLinks):
        print("There has been an error with an outdated link.")
        responses = list(
            filter(lambda x: x.url not in wrongResponseLinks, responses))
        actualProductLinks = list(
            filter(lambda x: x not in wrongResponseLinks, productsLinks))
        updateLinks(productsLinks)
        print("Remove outdated link, links list updated.")

    for i in responses:
        mainPacketsList.append(productBuilder(i))

    mainPacketsList = list(filter(lambda x: x is not None, mainPacketsList))

    with open(dataFilename, 'w+') as file:
        json.dump(mainPacketsList, file, indent=2, ensure_ascii=False)
    with open(backupFilename, 'w+') as file:
        backup = [{"link": linksSlice[-1]}]
        json.dump(backup, file, indent=2, ensure_ascii=False)
