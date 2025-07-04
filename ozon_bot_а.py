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

    # Read into DataFrame and process items as in old version
    if path.lower().endswith(('.xlsx', '.xls')):
        # Чтение Excel с заголовком на 4-й строке (header=3)
        df = pd.read_excel(path, header=3)
        # Фильтрация нужных столбцов
        try:
            df_filtered = df[['Группа', 'Цена', 'СКЛАД']].copy()
        except KeyError:
            await message.answer("В файле отсутствуют столбцы 'Группа', 'Цена' или 'СКЛАД'.")
            await state.clear()
            return
        df_filtered.columns = ['Артикул', 'Цена', 'Кол-во']
        # Убираем пустые артикула и цены
        df_filtered_cleaned = df_filtered.dropna(subset=['Артикул', 'Цена'])
        # Приводим к числу
        df_filtered_cleaned['Цена'] = pd.to_numeric(df_filtered_cleaned['Цена'], errors='coerce')
        df_filtered_cleaned['Кол-во'] = pd.to_numeric(df_filtered_cleaned['Кол-во'], errors='coerce').fillna(0).astype(int)
        # Сохраняем в новый файл
        output_path = f'updated_products_{user_id}.xlsx'
        df_filtered_cleaned.to_excel(output_path, index=False)
        user_data[user_id]['file_path'] = output_path
    else:
        # Для CSV используем общий подход
        try:
            df = pd.read_csv(path, sep=None, engine='python')
        except UnicodeDecodeError:
            df = pd.read_csv(path, sep=None, engine='python', encoding='cp1251')
        # Оставляем generic cleaning для CSV
        df = df[['Группа', 'Цена', 'СКЛАД']].dropna(subset=['Группа','Цена'])
        df.rename(columns={'Группа': 'Артикул', 'СКЛАД': 'Кол-во'}, inplace=True)
        df['Цена'] = pd.to_numeric(df['Цена'], errors='coerce')
        df['Кол-во'] = pd.to_numeric(df['Кол-во'], errors='coerce').fillna(0).astype(int)
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

    df = pd.read_excel(path)

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
            # kgt_ids = await fetch_kgt_set(session, offer_ids)
            kgt_ids = ['4FN50162B1A', '496B1811A', '4FN65170AE', '496B2717A', '496H6404WLA', '48SGMP970DA', '48SHT9803A', '48SGV970NA', '496H4403WLA', '4FN3220HAA', '4FN80200BE', '43FCR0303E', '48SGM970CA', '48SGV970GA', '4FP50165CA', '48SGV9853A1', '48SGM9864A', '4FP50166AA', '496ST36012WLA', '452CT420AE', '43HT0308E', '4FNA10250BE', '49480843WLA', '49623609WLA', '4FN65158E', '48SHT03A', '49623618WLA', '43FCR0304E', '49480472WLA', '496B1211A', '4FN32250AE', '4FN80200AE', '452CT420BE', '4F65125XBE', '4FN3220HBA', '4FP40250BA', '4F50163XAE', '4FN65165AE', '48SHT07A', '49624414WLA', '4FN50165BE', '4FP65125BA', '4FN80250BE', '4FNA10200CE', '4FP50165BA', '49623613WLA', '48SGY9861A1', '48SGM9854A', '496H6410WLA', '4FN50161AA', '4FN65250AE', '4FNA10250AE', '496B2714A', '4FN50163CA', '48SGM9852A1', '48SHT08A', '48SGY9864A', '48SHT9803A1', '4FN40250CE', '43HT1308E', '496H4409WLA', '496H3408WLA', '4FN40203AE', '48SGV9861A', '4FN50168E', '496B2707NA', '4FN32203AE', '4FP32203BA', '4FNA10160BNE', '496B1809A', '4FN65250BE', '48SGV9704BA', '4FP80160BA', '48SGV970RA', '49624412WLA', '49481232WLA', '4FP50172AA', '48SGY9852A1', '496B1221A', '496H3416WLA', '49481529WLA', '4FN32250CE', '4FN80160BE', '4FN50163AE', '43HT3152A', '4FP80160DA', '4FPA10160BNA', '496B2712A', '43HT0154E', '48SGD9812A1', '48SGM9853A1', '48SGD9813A', '48SGV970PA', '48SGVP980DE', '43HT1157E', '4F65125XAE', '494H1020WLA', '494L9324AX', '49623606WLA', '48SGVP970EA', '4FN40203BE', '496B1806A', '43HT1305E', '48SGQ9851A', '48SGV970LA', '452CT313BA', '4FP65160A', '496B2707A', '4FN65250CE', '48SGV9851A1', '48SGM970IA', '48SHT02A1', '4FN80250AE', '48SGY9853A1', '4FNA10160CNE', '4FN40158C1A1', '496ST36020WLA', '496H5413WLA', '4FP50163BA', '4FN32250BE', '4FN65125BE', '48SGVP980FE', '48SGV9864A', '4FN80160DE', '48SGV9854A', '496B1225A', '4FP80160CA', '49624416WLA', '48SGV9853A', '4FN50162C1A1', '4FN50167E', '49624420WLA', '49623611WLA', '4FP50163AA', '48SGMP980FE', '496H5406WLA', '496B1218A', '4FN50165CE', '48SGD9814A', '494H1822WLA', '48SGY9852A', '48SGV9704AA', '496H6414WNA', '496H6408WLA', '4FN65165BE', '4FN50172AE', '4FPA10160ANA', '48SGV970MA', '4FN32159B1A1', '48SGD9812A', '4FP50168A', '48SGY947AA1', '494H1028WLA', '48SGY9854A', '48SGM9863A1', '496B1228A', '43FCR0153A', '49480454WLA', '48SGV9852A1', '4FN50163BE', '494L9135AX', '49481539WLA', '48SGY9853A', '496H6417WLA', '48SGV9861A1', '496H6407WLA', '48SGQ9853A', '48SGY9862A', '48SGV970QA', '496H6405WLA', '49623604WLA', '48SGM9853A', '496B1815A', '4FN32160A1A', '4FN50166AE', '48SGQ9852A', '452CT383CA', '48SGQ9851A1', '48SGV9851A', '496B1822A', '43FCR1153A', '49624425WLA', '4FN40250AE', '49480643WLA', '494H1421WLA', '4FN80160AE', '452CT393BE', '496B1818A', '496B2720A', '49624405WLA', '48SGV9862A', '48SGD9813A1', '49624408WLA', '48SGVP970DA', '496H6403WLA', '49623615WLA', '494L9229AX', '43HT0157E', '48SGM9851A1', '48SGY9851A1', '48SGMP970EA', '48SGY9861A', '4FP65125AA', '49624406WLA', '496B1826A', '4F65125XCA', '48SGQ9864A', '4FN65125CA', '496B2705A', '48SGM970HA', '496ST36017WLA', '4FP65158A', '496B2710A', '49624404WLA', '48SGMP970CA', '496B1804A', '452CT353AE', '496H3406WLA', '48SGM9863A', '452CT403AE', '48SGQ9853A1', '4FN65160E', '48SGM970EA', '4FP65250BA', '43HT0155E', '48SGY9863A1', '496B2704A', '48SGV970HA', '4FN65125AE', '48SGM9851A', '4FN40250BE', '4FP32250CA', '496H5404WLA', '43FCR0155E', '48SGV9863A', '48SGVP970CA', '494H1816WLA', '48HT00307A1', '4FP50170A', '48SGVP980HE', '4FN50165AE', '4FP65170AA', '48SHT9804A', '494H1830WLA', '48SGM970DA', '48SGVP980GE', '4FN32203BE', '4FN50170E', '4FP65159A', '4FP80160AA', '48HT10806A', '4FP50165AA', '4FN40159B1A', '496B2710NA', '496H6414WLA', '4FNA10200AE', '4FP65165AA', '48SGM9852A', '48SGV9852A', '48SGY9863A', '494H1416WLA', '496B1215A', '49623607WLA', '48SGM970GA', '4FNA10160ANE', '48SGQ9863A', '496B2727A', '4FN40163AA', '48SGV9862A1', '494L9126AX', '49624410WLA', '48SGM9704AA', '4FP40203BA', '496H5405WLA', '496B1208A', '43HT1153A', '4FN50169E', '4FNA10200BE', '49623622WLA', '49623605WLA', '4FN80160CE', '496B1813A', '48SGV9704CA', '49480658WLA', '48SGY9851A', '43HT0305E', '4FP40250AA', '48SGQ9854A', '43FCR0152A', '496H3427WLA', '4FP32203AA', '48SGQ9852A1', '43HT1155E', '48SGQ9863A1', '43FCR0302A', '48SGV9863A1', '48SHT02A', '452CT303CA', '496B1811NA', '496B2708A', '4FN65159E', '48SGY9862A1', '4FN32203CA', '48SGQ93A0A1', '43FCR0154E', '496H5422WLA', '496H3405WLA', '494H1015WLA', '4F50163XBE']
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
            success = await send_update_batch(session, price_batch, stock_batch)
            if success:
                updated += len(price_batch.get("prices", []))

    await message.answer(
        f"Обновление завершено! Успешно обновлено {updated} позиций по курсу {rate}.",
        reply_markup=keyboard
    )

# Helper functions
async def fetch_all_offer_ids(
    session: aiohttp.ClientSession
) -> Set[str]:
    """
    Fetch all offer_ids from Ozon with pagination. Raises ValueError on bad request.
    """
    url = "https://api-seller.ozon.ru/v3/product/list"
    items = []
    last_id = ""
    while True:
        payload = {
            "filter": {"visibility": "ALL"},
            "last_id": last_id,
            "limit": 1000
        }
        try:
            async with session.post(url, json=payload) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except ClientResponseError as e:
            if e.status == 400:
                # Bad request, likely credentials or payload error
                raise ValueError("Ошибка запроса к Ozon (400 Bad Request). Проверьте Client-Id и Api-Key.") from e
            logging.error(f"HTTP error fetching offer ids: {e.status}")
            raise
        batch = data.get("result", {}).get("items", [])
        if not batch:
            break
        items.extend(batch)
        last_id = batch[-1].get("offer_id", "")
    return {item.get("offer_id") for item in items}

async def fetch_kgt_set(
    session: aiohttp.ClientSession,
    offer_ids: Set[str]
) -> Set[str]:
    url = "https://api-seller.ozon.ru/v3/product/info/list"
    async with session.post(
        url, json={"offer_id": list(offer_ids)}
    ) as resp:
        resp.raise_for_status()
        data = await resp.json()
    return {
        item["offer_id"]
        for item in data.get("items", [])
        if item.get("is_kgt")
    }

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
        if art not in valid_ids and art in kgt_ids:
            continue
        price_rub = int(row.get("Цена", 0) * rate)
        # warehouse = (
        #     1020002531538000
        #     if art in kgt_ids
        #     else 1020002390459000
        # )
        prices.append({
            "offer_id": art,
            "price": str(price_rub),
            "old_price": str(int(price_rub * 1.2))
        })
        stocks.append({
            "offer_id": art,
            "stock": row.get("Кол-во", 0),
            # "warehouse_id": warehouse
        })
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
        price_task = session.post(price_url, json=price_payload)
        stock_task = session.post(stock_url, json=stock_payload)
        price_resp, stock_resp = await asyncio.gather(price_task, stock_task)
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
