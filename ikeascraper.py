import requests
from bs4 import BeautifulSoup as bsp

# Scraping ikea means going by Section (Muebles) -> Subsection (Camas) -> Subsubsection (Camas tapizadas) -> Products (TUFJORD)
# Fetch the whole products list.
mainPage = requests.get(
    'https://www.ikea.com/es/es/cat/productos-products/')
catalogSections = bsp(mainPage.content, 'html.parser').find_all(
    'a', class_="vn-link vn-nav__link vn-accordion__image")

# Link for each of the main sections.
linksMainSections = list(map(lambda x: x.get('href'), catalogSections))
# This list will contain all the main subsections links.
subsectionLinks = []
for l in linksMainSections:
    sectionPage = requests.get(l)
    catalogSubsections = bsp(sectionPage.content, 'html.parser').find_all(
        'a', class_="vn-link vn__nav__link vn-6-grid-gap")
    subsectionLinks += list(map(lambda x: x.get('href'), catalogSubsections))
# Clean repeated links
