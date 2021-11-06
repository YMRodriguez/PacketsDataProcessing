import requests
import json
from bs4 import BeautifulSoup as bsp
from requests.exceptions import HTTPError
import os
import pathlib
from concurrent.futures import ThreadPoolExecutor
from itertools import zip_longest

# ---------------------- Links related functions --------------------------------------------


def getFullListOfProducts():
    # Scraping ikea means going by Section (Muebles) -> Subsection (Camas) -> Subsubsection (Camas tapizadas) -> Products (TUFJORD)
    # Fetch the whole products list.
    try:
        mainPage = requests.get(
            'https://www.ikea.com/es/en/cat/productos-products/')
        mainPage.raise_for_status()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')

    catalogSections = bsp(mainPage.content, 'html.parser').find_all(
        'a', class_="vn-link vn-nav__link vn-accordion__image")

    # Link for each of the main sections.
    linksMainSections = list(map(lambda x: x.get('href'), catalogSections))
    # Obtain the category id for every main section (furniture, kitchen&appliances...)
    mainSectionsIds = list(
        map(lambda x: x.split("-")[-1][:-1], linksMainSections))

    def getProductsFromMainSection(sectionId):
        maxProducts = 50000
        try:
            sectionProducts = requests.get(
                "https://sik.search.blue.cdtapps.com/es/en/product-list-page/more-products?category=" + sectionId + "&start=0&end=" + str(maxProducts))
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')
        return list(map(lambda x: x["pipUrl"], sectionProducts.json()["moreProducts"]["productWindow"]))
    # Gathering of all the unique products in the web.
    uniqueProductsLinks = set()
    for i in mainSectionsIds:
        uniqueProductsLinks.update(getProductsFromMainSection(i))

    pathlib.Path(os.path.dirname(__file__) + os.path.sep +
                 'ikeaData').mkdir(parents=True, exist_ok=True)
    updateLinks(uniqueProductsLinks)


def fetchLinks():
    with open(os.path.dirname(__file__) + os.path.sep + 'ikeaData' + os.path.sep + 'links.txt', 'r') as f:
        productsLinks = [line.rstrip('\n') for line in f]
    return productsLinks


def updateLinks(links):
    fullCatalogLinks = os.path.dirname(
        __file__) + os.path.sep + 'ikeaData' + os.path.sep + 'links.txt'
    with open(fullCatalogLinks, 'w') as file:
        for s in links:
            file.write(s + '\n')


try:
    productsLinks = fetchLinks()
except:
    print("Creating a new list of all the links in the ikea page.")
    getFullListOfProducts()
    productsLinks = fetchLinks()
    print("Fetched all links.")

# --------------------- Mappping functions ---------------------------------


def productToPackets(packageData, subgroupId):
    packets = []
    for i in range(packageData['quantity']['value']):
        packet = {}
        packet["id"] = ""
        packet["name"] = packageData["name"]
        packet["description"] = packageData["typeName"]
        packet["productId"] = int(
            packageData["articleNumber"]["value"].translate({ord("."): None}))
        packet["subgroupId"] = subgroupId
        if "Diameter" in list(map(lambda x: x["label"], packageData['measurements'][0])):
            packet["rounded"] = 1
            packet["length"], packet["weight"], packet["diameter"] = list(
                map(lambda x: x["value"].split(" ")[0], packageData["measurements"][0]))
        else:
            packet["rounded"] = 0
            packet["width"], packet["height"], packet["length"], packet["weight"] = list(
                map(lambda x: x["value"].split(" ")[0], packageData["measurements"][0]))
        packets.append(packet)
    return packets


def productBuilder(linkResponse):
    soup = bsp(linkResponse.content, 'html.parser')
    packagingDataSource = json.loads(soup.find(
        'div', class_="js-product-information-section range-revamp-product-information-section").get('data-initial-props'))
    packagingData = packagingDataSource['productDetailsProps'][
        'accordionObject']['packaging']['contentProps']['packages']
    # Get the names of the subproducts.
    packets = []
    subgroupId = int(packagingData[0]['articleNumber']
                     ['value'].translate({ord("."): None}))
    # Case where there are several product forming a product itself.
    if len(packagingData) > 1:
        # Got to do this distiction because first 'measurements' key is empty in a combined product.
        for i in packagingData[1:]:
            packets.extend(productToPackets(i, subgroupId))
    else:
        packets.extend(productToPackets(packagingData[0], subgroupId))
    return packets


# -------------- Main --------------------------------------
mainPacketsList = []
dataFilename = os.path.dirname(
    __file__) + os.path.sep + 'ikeaData' + os.path.sep + 'data.json'
backupFilename = os.path.dirname(
    __file__) + os.path.sep + 'ikeaData' + os.path.sep + 'from.json'


def get_url(url):
    return requests.get(url)


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)

# ----- Scrapper API altenative ---------------
# def get_url_ScrapperAPI(payload):
#    return requests.get('http://api.scraperapi.com', params=payload)
# maxApiRequestsAvailable = 4900
# apikeyChangerPivot = (maxApiRequestsAvailable - maxApiConcurrentCalls)/maxApiConcurrentCalls


maxApiConcurrentCalls = os.cpu_count()*5


def chargeCurrentMaxLinkIndex():
    try:
        with open(os.path.dirname(__file__) + os.path.sep + 'ikeaData' + os.path.sep + 'from.json', 'r') as f:
            currentMaxLinkIndex = productsLinks.index(
                json.load(f)[0]["link"]) + 1
        return currentMaxLinkIndex
    except:
        return 0


currentStartingLinkIndex = chargeCurrentMaxLinkIndex()
# Reload current extracted data.
if currentStartingLinkIndex:
    with open(os.path.dirname(__file__) + os.path.sep + 'ikeaData' + os.path.sep + 'data.json', 'r') as f:
        mainPacketsList = json.load(f)
for linksSlice in grouper(productsLinks[currentStartingLinkIndex:], maxApiConcurrentCalls, None):
    # ---------- Scraper API alternative, slow unfortunatelly --------------------
    # Configure ScrapperAPI
    # Create an account in ScrapperAPI and get the API key. It is valid for 5k requests.
    #apiKey = '<value>' if count < apikeyChangerPivot else '<value>'
    #payloads = list(map(lambda x: {'api_key': apiKey, 'url': x}, linksSlice))
    linksSlice = list(filter(lambda x: x is not None, linksSlice))
    responses = []
    try:
        with ThreadPoolExecutor(max_workers=maxApiConcurrentCalls) as pool:
            responses = list(pool.map(get_url, linksSlice))
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')

    # There is need to verify that the url is currently supported, example of failure (https://www.ikea.com/es/en/p/vattlosa-wall-decoration-home-black-40473610/)
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
        print("Links list updated, remove outdated link.")

    for i in responses:
        mainPacketsList.extend(productBuilder(i))

    with open(dataFilename, 'w+') as file:
        json.dump(mainPacketsList, file, indent=2, ensure_ascii=False)
    with open(backupFilename, 'w+') as file:
        backup = [{"link": linksSlice[-1]}]
        json.dump(backup, file, indent=2, ensure_ascii=False)
