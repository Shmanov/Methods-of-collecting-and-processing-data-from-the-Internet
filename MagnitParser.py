import os
import datetime as dt
from dotenv import load_dotenv
import requests
from urllib.parse import urljoin
import bs4
import pymongo

MONTHS = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}

load_dotenv(".env")
myclient = pymongo.MongoClient(os.getenv("DATA_BASE_URL"))


def save(data):
    mydb = myclient["magnit_product"]
    mycol = mydb["customers"]
    mycol.insert_one(data)

def date_parse(date_str: str):
    try:
        date_list = date_str.replace("с ", "", 1).replace("\n", "").split("до")
        first_mount = 0
        for date in date_list:
            temp_date = date.split()
            month = MONTHS[temp_date[1][:]]
            day = int(temp_date[0])

            if first_mount > int(month):      # Проверка на случай если акция переходит с одного года на другой
                year = dt.datetime.now().year + 1
            else:
                year = dt.datetime.now().year
                first_mount = int(month)
            yield dt.datetime(year, month, day)
    except AttributeError: yield (None)


url_start = "https://magnit.ru/promo/?geo=moskva"
response = requests.get(url_start).text
soup = bs4.BeautifulSoup(response, 'lxml')
catalog_main = soup.find("div", attrs={"class": "сatalogue__main"})


for product_tag in catalog_main.find_all("a", attrs={"class": "card-sale"}, reversive=False):
    data = {}

    data['url'] = product_tag.attrs.get("href")

    try:
        data['promo_name'] = str(product_tag.find("div", attrs={"class": "card-sale__header"}).text)
    except (AttributeError, TypeError): data['promo_name'] = None

    try:
        data['product_name'] = str(product_tag.find("div", attrs={"class": "card-sale__title"}).text)
    except (AttributeError, TypeError): data['product_name'] = None

    try:
        data['old_price'] = float(".".join(itm for itm in product_tag.find("div", attrs={"class": "label__price_old"}).text.split()))
    except (AttributeError, ValueError, TypeError): data['old_price'] = None

    try:
        data['new_price'] = float(".".join(itm for itm in product_tag.find("div", attrs={"class": "label__price_new"}).text.split()))
    except (AttributeError, ValueError, TypeError): data['new_price']  = None

    data['image_url'] = urljoin(url_start, product_tag.find("img").attrs.get("data-src"))

    try:
        date_str = product_tag.find("div", attrs={"class": "card-sale__date"}).text
    except (AttributeError, TypeError): date_str = None

    data['date_from'] = next(date_parse(date_str))
    data['date_to'] = next(date_parse(date_str))

    save(data)

