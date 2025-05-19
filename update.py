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

async def fetch_all_products():
    url_list = "https://api-seller.ozon.ru/v3/product/list"
    url_info = "https://api-seller.ozon.ru/v3/product/info/list"
    all_offer_ids = []
    last_id = ""
    async with aiohttp.ClientSession() as session:
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

        products = []
        for i in range(0, len(all_offer_ids), 1000):
            batch = all_offer_ids[i:i+1000]
            async with session.post(url_info, headers=HEADERS, json={"offer_id": batch}) as resp:
                info = await resp.json()
                items = info.get("items", [])
                products.extend(items)

        return products


async def main():
    products = await fetch_all_products()

    filtered_products = [p for p in products if p.get("type_id") == target_type_id]

    print(f"🔍 Найдено товаров с type_id {target_type_id}: {len(filtered_products)}")

    # Сохраняем в Excel
    df = pd.DataFrame([{
        "offer_id": p["offer_id"],
        "name": p["name"],
        "type_id": p["type_id"]
    } for p in filtered_products])
    df.to_excel("filtered_products.xlsx", index=False)
    print("✅ Результаты сохранены в filtered_products.xlsx")

    # Обновляем категорию
    await update_category(filtered_products)

if __name__ == "__main__":
    asyncio.run(main())