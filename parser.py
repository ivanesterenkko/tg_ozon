import os
import aiohttp
import asyncio
import pandas as pd
import ssl
import requests
from bs4 import BeautifulSoup
import re
from dotenv import load_dotenv
from tqdm import tqdm
import logging
import time

load_dotenv()

OZON_API_KEY = os.getenv("OZON_API_KEY")
OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID")

HEADERS = {
    "Client-Id": OZON_CLIENT_ID,
    "Api-Key": OZON_API_KEY,
    "Content-Type": "application/json"
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_all_products():
    url_list = "https://api-seller.ozon.ru/v3/product/list"
    all_offer_ids = []
    last_id = ""

    async with aiohttp.ClientSession() as session:
        while True:
            payload = {"filter": {"visibility": "ALL"}, "last_id": last_id, "limit": 1000}
            async with session.post(url_list, headers=HEADERS, json=payload) as resp:
                data = await resp.json()
                items = data.get("result", {}).get("items", [])
                if not items:
                    break
                all_offer_ids.extend([item["offer_id"] for item in items])
                last_id = data["result"].get("last_id", "")
                if not last_id:
                    break

    return set(all_offer_ids)

def load_artikuls(file_path):
    df = pd.read_excel(file_path, header=3)
    df_filtered = df[['Группа']]
    df_filtered.columns = ['Артикул']
    df_filtered_cleaned = df_filtered.dropna(subset=['Артикул'])
    return [str(value).strip() for value in df_filtered_cleaned['Артикул']]

def search_product(article, category_id):
    search_url = f"https://atpump.ru/search/?query={article}"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        with requests.Session() as session:
            session.headers.update(headers)
            session.verify = False
            response = session.get(search_url)
            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.text, "html.parser")
            product_blocks = soup.select(".product-list__item")

            for item in product_blocks:
                code_div = item.select_one(".product-code")
                if code_div:
                    match = re.search(r"Артикул[:\s]*([A-Z0-9\-]+)", code_div.get_text())
                    site_article = match.group(1).strip() if match else None
                    if site_article and article == site_article:
                        link_tag = item.select_one("a.product-list__name")
                        if link_tag and link_tag.has_attr("href"):
                            product_url = f"https://atpump.ru{link_tag['href']}"
                            product_response = session.get(product_url)
                            if product_response.status_code != 200:
                                continue

                            product_soup = BeautifulSoup(product_response.text, "html.parser")
                            title_tag = product_soup.find("div", class_="content-head__title").find("h1")
                            title = title_tag.text.strip() if title_tag else ""

                            price_tag = product_soup.find("div", class_="price")
                            price = price_tag["data-price"].strip() if price_tag and price_tag.has_attr("data-price") else price_tag.text.strip() if price_tag else "0"

                            description_tag = product_soup.find("div", class_="product-card__description")
                            description = [s.replace('\n', ' ') for s in description_tag.stripped_strings] if description_tag else []

                            tech_description_tag = description_tag.find("a", href=True, string=re.compile("Скачать техническое описание")) if description_tag else None
                            if tech_description_tag:
                                description.append(f"Ссылка на тех. описание: {tech_description_tag['href']}")

                            features_table = product_soup.find("table", class_="product_features")
                            attributes = []
                            if features_table:
                                for row in features_table.find_all("tr", class_="product_features-item"):
                                    feature_name = row.find("td", class_="product_features-title")
                                    feature_value = row.find("td", class_="product_features-value")
                                    if feature_name and feature_value:
                                        attributes.append({"id": feature_name.text.strip(), "value": feature_value.text.strip()})

                            images = []
                            image_tags = product_soup.find_all("a", class_="js-product-image-popup", href=True)
                            for img in image_tags:
                                img_url = img["href"]
                                if img_url and img_url.startswith("/"):
                                    img_url = "https://atpump.ru" + img_url
                                images.append(img_url)

                            return {
                                "offer_id": article,
                                "name": title,
                                "price": int(price.replace(" ", "")),
                                "currency_code": "RUB",
                                "vat": "0",
                                "type_id": category_id,
                                "description": description,
                                "images": images,
                                "attributes": attributes
                            }

    except Exception as e:
        logger.error(f"Ошибка при обработке артикула {article}: {e}")
        return None

def clean_value(value):
    units_to_remove = [" В", " м", " кг", " л", " мм", " см", " г", " м³", " Вт", " бар", "кВт"]
    value = value.replace(',', '.')
    for unit in units_to_remove:
        if value.endswith(unit):
            if unit == "кВт" or unit == " кг":
                return str(int(float(value.replace(unit, "").strip()) * 1000))
            else:
                return str(int(float(value.replace(unit, "").strip())))
    return value

def format_for_ozon(data):
    ozon_attributes = {
        'Диаметр': 12508,
        'Диаметр прохождения твердых частиц': None,
        'Применение': None,
        'Длина кабеля': 5391,
        'Напряжение': 8542,
        'Вес': 'weight',
        'Степень защиты': 5269,
        'Материал ведущего вала': None,
        'Мощность': 4851,
        'Ширина': 'width',
        'Высота': 'height',
        'Максимальная глубина погружения': 7464,
        'Уровень остатка воды': None,
        'Максимальный напор': 7465,
        'Длина': 'depth',
        'Тип насоса': 8229,
        'Максимальная подача': None,
        'Производитель двигателя': None,
        'Поплавковый выключатель': None,
        'Соединение': None,
        'Материал корпуса': 5156,
        'Рабочее колесо': None,
        'Опорное колено': None
    }

    formatted_items = []
    for item in data.get("items", []):
        formatted_item = {
            "attributes": [],
            "barcode": "",
            "description_category_id": 83625738,
            "new_description_category_id": 0,
            "color_image": "",
            "complex_attributes": [],
            "currency_code": "RUB",
            "depth": 120,
            "dimension_unit": "mm",
            "height": 120,
            "images": item.get("images", []),
            "images360": [],
            "name": item.get("name", ""),
            "offer_id": item.get("offer_id", ""),
            "old_price": str(int(int(item.get("price", 0)) * 1.2)),
            "pdf_list": [],
            "price": str(item.get("price", 0)),
            "primary_image": "",
            "type_id": item.get("type_id", ""),
            "vat": "0.2",
            "weight": 2500,
            "weight_unit": "g",
            "width": 120
        }

        for attribute in item.get("attributes", []):
            attr_id = ozon_attributes.get(attribute["id"])
            if attr_id is None:
                continue
            cleaned_value = clean_value(attribute["value"])
            if attr_id == 'weight':
                formatted_item["weight"] = int(cleaned_value)
            elif attr_id == 'width':
                formatted_item["width"] = int(cleaned_value)
            elif attr_id == 'height':
                formatted_item["height"] = int(cleaned_value)
            elif attr_id == 'depth':
                formatted_item["depth"] = int(cleaned_value)
            elif attr_id:
                formatted_item["attributes"].append({
                    "complex_id": 0,
                    "id": attr_id,
                    "values": [{"value": cleaned_value}]
                })

        formatted_item["attributes"].append({
            "complex_id": 0,
            "id": 9048,
            "values": [{"value": item.get("name", "")}]
        })
        formatted_item["attributes"].append({
                    "complex_id": 0,
                    "id": 85,
                    "values": [{"value": "Pedrollo"}]
                })

        formatted_items.append(formatted_item)

    return {"items": formatted_items}


def upload_to_ozon(products_data):
    OZON_API_URL = "https://api-seller.ozon.ru/v3/product/import"
    response = requests.post(OZON_API_URL, headers=HEADERS, json=products_data)

    if response.status_code == 200:
        logger.info("Товары успешно загружены!")
    else:
        logger.error(f"Ошибка загрузки: {response.status_code}")
        logger.error(response.text)

async def main():
    ozon_artikuls = await fetch_all_products()
    artikuls = set(load_artikuls("остатки.XLSX"))
    data = {"items": []}
    categories_id = {
        'А   Дренажные насосы': 91462,
        'Б   Колодезные насосы': 970731315,
        'В   Фекальные насосы': 98338,
        'Г   Скважинные насосы': 970731316,
        'Д   Самовсасывающие насосы': 91466,
        'Е   Вихревые насосы': 91471,
        'Ж   Центробежные': 91466,
        'З   Многоступенчатые': 91471,
        'К   Нас. авт.станц': 'del',
        'Л   Нас. авт.станц. с защ. с/сх': 'del',
        'М   Баки': 'del',
        'Н   Аксессуары': 'del',
        'О   Пульты': 99332,
        'П   Станции управления': 99332,
        'Р   Комбипрессы': 'del',
        'С   Установка SAR': 'del',
        '*   Гидравлика': 'del',
        '*   Электродвигатели': 'del',
        'Артикул': 'del'
    }

    category = 'del'
    for article in tqdm(artikuls, desc="Обработка артикулов", unit="товар"):
        if article in categories_id:
            category = categories_id[article]
        elif category == 'del' or article in ozon_artikuls:
            continue
        else:
            product_info = search_product(article, category_id=category)
            if product_info:
                data["items"].append(product_info)
            time.sleep(0.1)

        if len(data["items"]) >= 100:
            ozon_data = format_for_ozon(data)
            upload_to_ozon(ozon_data)
            data = {"items": []}
            logger.info("100 товаров отгружены")

    if data["items"]:
        ozon_data = format_for_ozon(data)
        upload_to_ozon(ozon_data)


if __name__ == '__main__':
    asyncio.run(main())
