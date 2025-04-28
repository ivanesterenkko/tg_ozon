import os
import asyncio
import logging
from dotenv import load_dotenv
from typing import Any, Dict, Set, Tuple, AsyncGenerator

import pandas as pd
import ssl

import aiohttp
from aiohttp import ClientResponseError, TCPConnector, ClientTimeout

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

# Load configuration from .env
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OZON_API_KEY = os.getenv("OZON_API_KEY")
OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID")

if not all([TELEGRAM_BOT_TOKEN, OZON_API_KEY, OZON_CLIENT_ID]):
    logging.critical(
        "Missing environment variables: TELEGRAM_BOT_TOKEN, OZON_API_KEY, OZON_CLIENT_ID"
    )
    exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Initialize bot and dispatcher with FSM storage
bot = Bot(token=TELEGRAM_BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# FSM States
class Form(StatesGroup):
    waiting_rate = State()
    waiting_file = State()

# Keyboard
keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="Ввести курс евро к рублю"),
            KeyboardButton(text="Добавить файл с товарами"),
            KeyboardButton(text="Обновить товары")
        ]
    ],
    resize_keyboard=True
)

# In-memory user data
user_data: Dict[int, Dict[str, Any]] = {}

# Ozon API headers
OZON_HEADERS = {
    "Client-Id": OZON_CLIENT_ID,
    "Api-Key": OZON_API_KEY,
    "Content-Type": "application/json"
}

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    user_data[user_id] = {"exchange_rate": None, "file_path": None}
    await message.answer(
        "Привет! Введите курс евро к рублю и загрузите файл с товарами, чтобы обновить их на Ozon.",
        reply_markup=keyboard
    )

@dp.message(lambda m: m.text == "Ввести курс евро к рублю")
async def cmd_set_rate(message: types.Message, state: FSMContext):
    await state.set_state(Form.waiting_rate)
    await message.answer(
        "Пожалуйста, введите курс евро к рублю (например, 80.5):",
        reply_markup=types.ReplyKeyboardRemove()
    )

@dp.message(Form.waiting_rate)
async def process_rate(message: types.Message, state: FSMContext):
    text = message.text.replace(",", ".")
    try:
        rate = float(text)
        if rate <= 0:
            raise ValueError
    except ValueError:
        await message.answer("Некорректный курс. Введите число больше нуля.")
        return

    user_id = message.from_user.id
    user_data[user_id]["exchange_rate"] = rate
    await state.clear()
    await message.answer(f"Курс сохранён: {rate}", reply_markup=keyboard)

@dp.message(lambda m: m.text == "Добавить файл с товарами")
async def cmd_add_file(message: types.Message, state: FSMContext):
    await state.set_state(Form.waiting_file)
    await message.answer(
        "Пришлите файл с товарами (.xlsx, .xls или .csv).",
        reply_markup=types.ReplyKeyboardRemove()
    )

@dp.message(Form.waiting_file, F.document)
async def process_file(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    doc = message.document
    fname = doc.file_name.lower()
    if not fname.endswith((".xlsx", ".xls", ".csv")):
        await message.answer(
            "Неподдерживаемый формат. Отправьте .xlsx, .xls или .csv."
        )
        return

    save_dir = "user_files"
    os.makedirs(save_dir, exist_ok=True)
    path = os.path.join(save_dir, f"{user_id}_{doc.file_name}")

    # Download the file
    file = await bot.get_file(doc.file_id)
    await bot.download_file(file.file_path, destination=path)

    # Process Excel
    if fname.endswith(('.xlsx', '.xls')):
        try:
            df = pd.read_excel(path, header=3)
            df_filtered = df[['Группа', 'Цена', 'СКЛАД']].copy()
        except Exception as e:
            await message.answer(f"Ошибка обработки Excel: {e}")
            await state.clear()
            return
        df_filtered.columns = ['Артикул', 'Цена', 'Кол-во']
        df_filtered.dropna(subset=['Артикул', 'Цена'], inplace=True)
        df_filtered.loc[:, 'Цена'] = pd.to_numeric(df_filtered['Цена'], errors='coerce')
        df_filtered.loc[:, 'Кол-во'] = pd.to_numeric(df_filtered['Кол-во'], errors='coerce').fillna(0).astype(int)
        output_path = f'updated_products_{user_id}.xlsx'
        df_filtered.to_excel(output_path, index=False)
        user_data[user_id]['file_path'] = output_path
    else:
        # Process CSV
        try:
            df = pd.read_csv(path, sep=None, engine='python')
        except UnicodeDecodeError:
            df = pd.read_csv(path, sep=None, engine='python', encoding='cp1251')
        try:
            df = df[['Группа', 'Цена', 'СКЛАД']].dropna(subset=['Группа', 'Цена'])
        except KeyError:
            await message.answer("В CSV отсутствуют столбцы 'Группа', 'Цена' или 'СКЛАД'.")
            await state.clear()
            return
        df.rename(columns={'Группа': 'Артикул', 'СКЛАД': 'Кол-во'}, inplace=True)
        df.loc[:, 'Цена'] = pd.to_numeric(df['Цена'], errors='coerce')
        df.loc[:, 'Кол-во'] = pd.to_numeric(df['Кол-во'], errors='coerce').fillna(0).astype(int)
        output_path = f'updated_products_{user_id}.xlsx'
        df.to_excel(output_path, index=False)
        user_data[user_id]['file_path'] = output_path

    await state.clear()
    await message.answer(
        "Файл успешно загружен и обработан.",
        reply_markup=keyboard
    )

@dp.message(lambda m: m.text == "Обновить товары")
async def cmd_update_products(message: types.Message):
    user_id = message.from_user.id
    data = user_data.get(user_id, {})
    rate = data.get("exchange_rate")
    path = data.get("file_path")

    if not rate:
        await message.answer("Сначала введите курс евро к рублю!")
        return
    if not path or not os.path.exists(path):
        await message.answer("Сначала загрузите файл с товарами!")
        return

    try:
        df = pd.read_excel(path)
    except Exception as e:
        await message.answer(f"Не удалось прочитать файл: {e}")
        return

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    connector = TCPConnector(ssl=ssl_ctx, limit=20)
    timeout = ClientTimeout(total=60)

    async with aiohttp.ClientSession(
        headers=OZON_HEADERS,
        connector=connector,
        timeout=timeout
    ) as session:
        try:
            offer_ids = await fetch_all_offer_ids(session)
            kgt_ids = await fetch_kgt_set(session, offer_ids)
        except ValueError as e:
            await message.answer(str(e))
            return
        except ClientResponseError as e:
            logging.error(f"HTTP error: {e.status} {e.message}")
            await message.answer(f"Ошибка при обращении к Ozon: {e.status}. Попробуйте позже.")
            return
        except Exception:
            logging.exception("Unexpected error при получении данных от Ozon")
            await message.answer("Не удалось получить данные от Ozon.")
            return

        updated = 0
        async for price_batch, stock_batch in generate_batches(df, rate, offer_ids, kgt_ids):
            if await send_update_batch(session, price_batch, stock_batch):
                updated += len(price_batch.get("prices", []))

    await message.answer(
        f"Обновление завершено! Успешно обновлено {updated} позиций по курсу {rate}.",
        reply_markup=keyboard
    )

# Helper functions
async def fetch_all_offer_ids(
    session: aiohttp.ClientSession
) -> Set[str]:
    url = "https://api-seller.ozon.ru/v3/product/list"
    items: list = []
    last_id: str = ""
    while True:
        payload = {"filter": {"visibility": "ALL"}, "last_id": last_id, "limit": 1000}
        async with session.post(url, headers=OZON_HEADERS, json=payload) as resp:
            text = await resp.text()
            if resp.status == 400:
                raise ValueError(f"Ошибка запроса к Ozon (product/list): {text}")
            resp.raise_for_status()
            data = await resp.json()
        result = data.get("result", {})
        batch = result.get("items", [])
        if not batch:
            break
        items.extend(batch)
        last_id = result.get("last_id", "") or ""
        if not last_id:
            break
    return {item.get("offer_id") for item in items}

async def fetch_kgt_set(
    session: aiohttp.ClientSession,
    offer_ids: Set[str]
) -> Set[str]:
    """
    Fetch detailed info for given offer_ids in batches and return set of KGT offer_ids.
    Raises ValueError on bad request.
    """
    url = "https://api-seller.ozon.ru/v3/product/info/list"
    kgt_ids: Set[str] = set()
    # Split offer_ids into chunks of max 1000
    ids_list = list(offer_ids)
    for i in range(0, len(ids_list), 1000):
        batch_ids = ids_list[i:i + 1000]
        payload = {"offer_id": batch_ids}
        try:
            async with session.post(url, headers=OZON_HEADERS, json=payload) as resp:
                text = await resp.text()
                if resp.status == 400:
                    raise ValueError(f"Ошибка запроса к Ozon (product/info): {text}")
                resp.raise_for_status()
                data = await resp.json()
        except ClientResponseError as e:
            logging.error(f"HTTP error fetching product info: {e.status}")
            raise
        items = data.get("items", []) or []
        for item in items:
            if item.get("is_kgt"):
                kgt_ids.add(item.get("offer_id"))
    return kgt_ids

async def generate_batches(
    df: pd.DataFrame,
    rate: float,
    valid_ids: Set[str],
    kgt_ids: Set[str],
    batch_size: int = 100
) -> AsyncGenerator[Tuple[Dict[str, Any], Dict[str, Any]], None]:
    prices, stocks = [], []
    for _, row in df.iterrows():
        art = str(row.get("Артикул", "")).strip()
        if art not in valid_ids:
            continue
        price_rub = int(row.get("Цена", 0) * rate)
        warehouse = 1020002531538000 if art in kgt_ids else 1020002390459000
        prices.append({"offer_id": art, "price": str(price_rub), "old_price": str(int(price_rub * 1.2))})
        stocks.append({"offer_id": art, "stock": row.get("Кол-во", 0), "warehouse_id": warehouse})
        if len(prices) >= batch_size:
            yield {"prices": prices}, {"stocks": stocks}
            prices, stocks = [], []
    if prices:
        yield {"prices": prices}, {"stocks": stocks}

async def send_update_batch(
    session: aiohttp.ClientSession,
    price_payload: Dict[str, Any],
    stock_payload: Dict[str, Any]
) -> bool:
    try:
        price_url = "https://api-seller.ozon.ru/v1/product/import/prices"
        stock_url = "https://api-seller.ozon.ru/v2/products/stocks"
        price_resp, stock_resp = await asyncio.gather(
            session.post(price_url, headers=OZON_HEADERS, json=price_payload),
            session.post(stock_url, headers=OZON_HEADERS, json=stock_payload)
        )
        price_resp.raise_for_status()
        stock_resp.raise_for_status()
        return True
    except ClientResponseError as e:
        logging.error(f"Ozon API error: {e.status} - {e.message}")
    except Exception:
        logging.exception("Unexpected error in send_update_batch")
    return False

if __name__ == "__main__":
    dp.run_polling(bot, skip_updates=True)
