import requests
import re
import math
import time
from bs4 import BeautifulSoup as bsp
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.relative_locator import locate_with


# Configuring selenium
options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument('--incognito')
options.add_argument('--headless')
driver = webdriver.Chrome(
    "./chromedriver", options=options)

# Scraping ikea means going by Section (Muebles) -> Subsection (Camas) -> Subsubsection (Camas tapizadas) -> Products (TUFJORD)
# Fetch the whole products list.
mainPage = requests.get(
    'https://www.ikea.com/es/es/cat/productos-products/')
catalogSections = bsp(mainPage.content, 'html.parser').find_all(
    'a', class_="vn-link vn-nav__link vn-accordion__image")

# Link for each of the main sections.
linksMainSections = list(map(lambda x: x.get('href'), catalogSections))


def getChildrenLinks(parentsLinks, reg):
    childrenLinks = []
    for l in parentsLinks:
        sectionPage = requests.get(l)
        # Makes a soup that contains the objects targeted by the reg expression.
        desiredRegObjects = bsp(sectionPage.content, 'html.parser').find_all(
            'a', class_=re.compile(reg))
        # Gets links under the specified class.
        childrenLinks += list(map(lambda x: x.get('href'), desiredRegObjects))
    return childrenLinks


# This will represent a unique list of subsection links
subsectionsLinks = list(dict.fromkeys(getChildrenLinks(
    linksMainSections, "vn-link vn__nav__link vn-")))


def getProductsFromSubsection(subsectionLinks):
    """
        This function gets all the links of the products in a subsection.

        :param: link of the subsection.
        :return: list of product links.
    """
    subsectionBasePage = requests.get(subsectionLinks)
    catalogBase = bsp(subsectionBasePage.content, 'html.parser')
    # Get all the expanded catalog if possible.
    try:
        count = str.strip(catalogBase.find(
            'div', class_="catalog-product-list__total-count").string).split(" ")
        maxProducts = int(count[-1])
        currentProducts = int(count[1])
        pages = math.floor(maxProducts/currentProducts)
        # Fetch the subsection page.
        driver.get(subsectionLinks + "?page=" + str(pages))
        time.sleep(3)
        page_source = driver.page_source
        catalogExpanded = bsp(page_source, 'html.parser')
    except:
        catalogExpanded = catalogBase
    productDivs = catalogExpanded.find_all(
        'div', class_="plp-fragment-wrapper")
    # Get the links and remove duplicates.
    productLinks = list(map(lambda x: x.find(
        'a', class_="range-revamp-product-compact__wrapper-link").get('href'), list(set(productDivs))))
    return productLinks


products = getProductsFromSubsection(subsectionsLinks[0])


def productToPackets(productName, packetData, subgroupId):
    print("a")
    print(packetData.get_attribute('innerHTML'))
    print("b")
    packets = []
    dimensionSpans = list(map(lambda x: x.text, packetData.find_elements(
        By.XPATH, ".//*[@class='range-revamp-product-details__label--bold']")))
    for i in range(int(dimensionSpans[4])):
        product = {}
        product["id"] = ""
        product["name"] = productName
        product["productId"] = packetData.find_element(
            By.XPATH, ".//*[@class='range-revamp-product-identifier__value']").text
        product["subgroupId"] = subgroupId
        # Dimension measures in centimetres and weight in kilograms.
        product["width"] = dimensionSpans[0].split(" ")[0]
        product["height"] = dimensionSpans[1].split(" ")[0]
        product["length"] = dimensionSpans[2].split(" ")[0]
        product["weight"] = dimensionSpans[3].split(" ")[0]
        packets.append(product)
    return packets


def productBuilder(link, subgroupId):
    driver.get(link)
    time.sleep(1)
    # Click on 'Product details'/'Detalles del producto" button.
    detailsButton = list(filter(lambda x: x.text == "Detalles del producto",
                                driver.find_elements(By.CLASS_NAME, "range-revamp-chunky-header__title")))[0].find_element(By.XPATH, "../..")
    driver.execute_script("arguments[0].click();", detailsButton)
    time.sleep(1)
    # Click on 'Packaging'/'Embalaje' button.
    packagingButton = list(filter(lambda x: x.text == "Embalaje", driver.find_elements(By.CLASS_NAME,
                                                                                       "range-revamp-accordion-item-header__title")))[0].find_element(By.XPATH, "../..")
    driver.execute_script("arguments[0].click();", packagingButton)
    time.sleep(3)
    packagingDiv = driver.find_element(
        By.ID, "SEC_product-details-packaging")
    print(packagingDiv.get_attribute('innerHTML'))
    subproductsNamesDivs = packagingDiv.find_elements(
        By.XPATH, ".//*[@class='range-revamp-product-details__header notranslate']")
    # Get the names of the subproducts.
    packets = []
    mainProductDivs = list(
        map(lambda x: x.find_element(By.XPATH, ".."), subproductsNamesDivs))
    for i in mainProductDivs:
        productName = i.find_element(
            By.XPATH, ".//*[@class='range-revamp-product-details__header notranslate']").get_attribute('innerHTML')
        packets.extend(list(map(lambda x: productToPackets(productName, x, subgroupId), i.find_elements(
            By.XPATH, ".//*[@class='range-revamp-product-details__container']"))))
    return packets


for p in products[:1]:
    pResponse = requests.get(p)
    pSoup = bsp(pResponse.content, 'html.parser')
    # Gets the first identifier that corresponds to the productId if there is an only item object.
    subgroupId = pSoup.find(
        "span", class_="range-revamp-product-identifier__value")
    pList = productBuilder(p, subgroupId)
