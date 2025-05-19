import os
import aiohttp
import asyncio
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
    all_offer_ids = []
    last_id = ""

    async with aiohttp.ClientSession() as session:
        while True:
            payload = {
                "filter": {"visibility": "ALL"},
                "last_id": last_id,
                "limit": 1000
            }

            async with session.post(url_list, headers=HEADERS, json=payload) as response:
                data = await response.json()

                items = data.get("result", {}).get("items", [])
                if not items:
                    break

                all_offer_ids.extend([item["offer_id"] for item in items])
                last_id = data["result"].get("last_id", "")

                # Если last_id пустой, значит все товары получены
                if not last_id:
                    break

        return all_offer_ids

async def main():
    all_offer_ids = await fetch_all_products()
    print(f"📦 Общее количество товаров на Ozon: {len(all_offer_ids)}")

if __name__ == "__main__":
    asyncio.run(main())