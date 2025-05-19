import os
import aiohttp
import asyncio
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

OZON_API_KEY = os.getenv("OZON_API_KEY")
OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID")

HEADERS = {
    "Client-Id": OZON_CLIENT_ID,
    "Api-Key": OZON_API_KEY,
    "Content-Type": "application/json"
}

def load_artikuls(file_path):
    """
    Считывает из Excel столбец 'Группа' (начиная с 4-й строки заголовков)
    и возвращает список строк-артикулов.
    """
    df = pd.read_excel(file_path, header=3)
    df_filtered = df[['Группа']]
    df_filtered.columns = ['Артикул']
    df_filtered_cleaned = df_filtered.dropna(subset=['Артикул'])
    return [str(value).strip() for value in df_filtered_cleaned['Артикул']]

async def fetch_all_products():
    """
    Постранично загружает все продукты из Ozon API (до 1000 за запрос),
    возвращает список словарей, каждый из которых содержит, в том числе,
    ключ 'offer_id'.
    """
    url_list = "https://api-seller.ozon.ru/v3/product/list"
    url_info = "https://api-seller.ozon.ru/v3/product/info/list"
    all_offer_ids = []
    last_id = ""

    async with aiohttp.ClientSession() as session:
        # Сначала получаем все offer_id
        while True:
            payload = {
                "filter": {"visibility": "ALL"},
                "last_id": last_id,
                "limit": 1000
            }
            async with session.post(url_list, headers=HEADERS, json=payload) as resp:
                data = await resp.json()
                items = data.get("result", {}).get("items", [])
                all_offer_ids.extend([item["offer_id"] for item in items])
                last_id = data["result"].get("last_id")
                if not last_id:
                    break

        # Затем подтягиваем полную информацию порциями по 1000 offer_id
        products = []
        for i in range(0, len(all_offer_ids), 1000):
            batch = all_offer_ids[i:i+1000]
            async with session.post(url_info, headers=HEADERS, json={"offer_id": batch}) as resp:
                info = await resp.json()
                products.extend(info.get("items", []))

        return products

async def main():
    # 1) Загружаем список артикулов из Excel
    excel_path = "остатки.XLSX"  # <-- укажите путь к вашему файлу
    artikuls = load_artikuls(excel_path)

    # 2) Загружаем все продукты с Ozon
    products = await fetch_all_products()

    # 3) Собираем множество offer_id из Ozon
    ozon_offer_ids = { str(p["offer_id"]) for p in products }

    # 4) Находим артикули, которых нет на Ozon
    missing = [sku for sku in artikuls if sku not in ozon_offer_ids]

    # 5) Выводим результат
    if missing:
        print("❗ Следующие артикулы есть в таблице, но не найдены на Ozon:")
        print("❗ Всего отсутствующих артикулов:", len(missing))
        for sku in missing:
            print(sku)
    else:
        print("✅ Все артикули из таблицы найдены на Ozon.")

    # (Опционально) Сохраняем в Excel
    df_missing = pd.DataFrame({"Артикул": missing})
    df_missing.to_excel("missing_on_ozon.xlsx", index=False)
    print("✅ Список отсутствующих артикулов сохранён в missing_on_ozon.xlsx")

if __name__ == "__main__":
    asyncio.run(main())
