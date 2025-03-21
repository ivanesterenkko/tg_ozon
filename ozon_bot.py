import asyncio
import os
import pandas as pd
import logging
import aiohttp
import ssl
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiohttp import TCPConnector

# Токены для работы с API и ботом
TELEGRAM_BOT_TOKEN = "7562716014:AAHAeV-yJ3PgbJwuGuibDRTkYNHUzhIC298"
OZON_API_KEY = "93797eb9-12b4-470b-8ecb-ef8ba3d80677"
OZON_CLIENT_ID = "2492604"

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

logging.basicConfig(level=logging.INFO)

# Создаем словарь для хранения данных каждого пользователя
user_data = {}

# Кнопки
button_exchange_rate = KeyboardButton(text="Ввести курс евро к рублю")
button_add_file = KeyboardButton(text="Добавить файл с товарами")
update_button = KeyboardButton(text="Обновить товары")

keyboard = ReplyKeyboardMarkup(
    keyboard=[[button_exchange_rate, button_add_file, update_button]],
    resize_keyboard=True
)

@dp.message(Command("start"))
async def start(message: types.Message):
    user_data[message.from_user.id] = {'exchange_rate': None, 'df': None}  # Инициализация данных для нового пользователя
    await message.answer("Привет! Данный введите курс евро и добавьте файл с товарами, чтобы обновить их в ЛК продавца!", reply_markup=keyboard)

# Ввод курса евро
@dp.message(lambda message: message.text == "Ввести курс евро к рублю")
async def enter_exchange_rate(message: types.Message):
    await message.answer("Пожалуйста, введите курс евро к рублю:", reply_markup=types.ReplyKeyboardRemove())

@dp.message(lambda message: message.text and message.text.replace('.', '', 1).isdigit())
async def set_exchange_rate(message: types.Message):
    user_id = message.from_user.id
    try:
        exchange_rate = float(message.text)

        if exchange_rate <= 0:
            await message.answer("Курс евро не может быть 0 или отрицательным! Пожалуйста, введите корректное значение курса (например, 80.5).")
        else:
            user_data[user_id]['exchange_rate'] = exchange_rate
            if user_data[user_id]['df'] is None:
                await message.answer(f"Курс сохранен! Составляет: {exchange_rate}. Теперь добавьте файл с товарами.", reply_markup=keyboard)
                return
            else:
                await message.answer(f"Курс сохранен! Составляет: {exchange_rate}. Теперь вы можете обновить товары, нажав на соответствующую кнопку.", reply_markup=keyboard)
    except ValueError:
        await message.answer("Ошибка: Пожалуйста, введите корректное значение курса (например, 80.5).")

# Загрузка файла с товарами
@dp.message(lambda message: message.text == "Добавить файл с товарами")
async def add_file(message: types.Message):
    await message.answer("Пожалуйста, отправьте файл с товарами (например, в формате .xlsx).")

@dp.message(F.document)
async def handle_file(message: types.Message):
    user_id = message.from_user.id
    if not message.document:
        await message.answer("Это не файл. Пожалуйста, отправьте файл с товарами.")
        return

    file_id = message.document.file_id
    file = await bot.get_file(file_id)
    file_path = file.file_path

    local_file_path = f"documents/file_{user_id}.xlsx"  # Уникальное имя для каждого пользователя

    try:
        file_content = await bot.download_file(file_path)
        file_data = file_content.read()
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        with open(local_file_path, "wb") as f:
            f.write(file_data)

        # Чтение файла и фильтрация данных
        df = pd.read_excel(local_file_path, header=3)
        df_filtered = df[['Группа', 'Цена', 'СКЛАД']]
        df_filtered.columns = ['Артикул', 'Цена', 'Кол-во']
        df_filtered_cleaned = df_filtered.dropna(subset=['Артикул', 'Цена'])
        df_filtered_cleaned.loc[:, 'Цена'] = pd.to_numeric(df_filtered_cleaned['Цена'], errors='coerce')
        df_filtered_cleaned.loc[:, 'Кол-во'] = pd.to_numeric(df_filtered_cleaned['Кол-во'], errors='coerce')
        output_path = f'updated_products_{user_id}.xlsx'
        df_filtered_cleaned.to_excel(output_path, index=False)

        user_data[user_id]['df'] = f'updated_products_{user_id}.xlsx'
        if user_data[user_id]['exchange_rate'] is None:
            await message.answer("Файл успешно загружен и обработан! А теперь введите курс евро к рублю!", reply_markup=keyboard)
            return
        else:
            await message.answer("Файл успешно загружен и обработан! Теперь вы можете обновить товары, нажав на соответствующую кнопку.", reply_markup=keyboard)

    except Exception as e:
        await message.answer(f"Произошла ошибка при загрузке или обработке файла: {e}")

# Обновление данных товаров
@dp.message(lambda message: message.text == "Обновить товары")
async def update_ozon_data(message: types.Message):
    user_id = message.from_user.id
    if user_data[user_id]['exchange_rate'] is None:
        await message.answer("Сначала введите курс евро к рублю!")
        return

    if user_data[user_id]['df'] is None:
        await message.answer("Файл с товарами не был загружен!")
        return
    df = pd.read_excel(user_data[user_id]['df'])
    updated_products = []

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    connector = TCPConnector(ssl=ssl_context)

    async with aiohttp.ClientSession(connector=connector) as session:
        batch_size = 100
        article_data = await get_ozon_products(session)
        if not article_data:
            await message.answer("Не удалось получить список товаров с Ozon.")
            return
        article_data = {item["offer_id"] for item in article_data}
        kgt = await get_product_info(session, article_data)
        logging.info(len(article_data))
        logging.info(len(kgt))
        if not kgt:
            await message.answer("Не удалось получить расширенный список товаров с Ozon.")
            return
        kgt = {item["offer_id"] for item in kgt if item.get("is_kgt", False)}
        rows = list(df.iterrows())
        logging.info(len(rows))
        categories_id = ['А   Дренажные насосы', 'Б   Колодезные насосы', 'В   Фекальные насосы', 'Г   Скважинные насосы', 'Д   Самовсасывающие насосы', 'Е   Вихревые насосы', 'Ж   Центробежные', 'З   Многоступенчатые', 'К   Нас. авт.станц', 'Л   Нас. авт.станц. с защ. с/сх', 'М   Баки', 'Н   Аксессуары', 'О   Пульты', 'П   Станции управления', 'Р   Комбипрессы', 'С   Установка SAR', '*   Гидравлика', '*   Электродвигатели', 'Артикул', 'Итого']
        updated_products = []
        k = 0
        e = 0
        logging.info(len(article_data))
        logging.info(len(kgt))
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            price_data = {
                        "prices": [],
                    }
            stock_data = {
                "stocks": [],
                }
            for _, row in batch:
                article = str(row["Артикул"]).strip()
                if article in categories_id:
                    continue
                if article not in article_data:
                    continue
                price_euro = row["Цена"]
                quantity = row["Кол-во"] if pd.notna(row["Кол-во"]) else 0
                price_rub = price_euro * user_data[user_id]['exchange_rate']
                price_data["prices"].append({
                    "offer_id": str(article),
                    "price": str(int(price_rub)),
                    "old_price": str(int(price_rub * 1.2))
                })
                k += 1
                e += 1
                if article in kgt:
                    warehouse_id = 1020002531538000
                else:
                    warehouse_id = 1020002390459000
                stock_data["stocks"].append({
                    "offer_id": str(article),
                    "stock": quantity,
                    "warehouse_id": warehouse_id
                })
            result = await update_ozon_batch(session, price_data, stock_data)
            if result:
                updated_products.append(f"Товары обновлены успешно.")
        logging.info(k)
        logging.info(e)
    if updated_products:
        await message.answer(f"Обновление завершено! Цены подсчитаны по курсу: {user_data[user_id]['exchange_rate']} ")
    else:
        await message.answer("Не удалось обновить товары. Проверьте данные и попробуйте снова.")


async def get_product_info(session, article_data):
    """ Получает данные о товарах с Ozon по списку артикулов (offer_id) асинхронно """
    url = "https://api-seller.ozon.ru/v3/product/info/list"

    headers = {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }
    payload = payload = {
        "offer_id": list(article_data)
    }

    try:
        async with session.post(url, headers=headers, json=payload) as response:
            response_text = await response.text()
            if response.status == 200:
                data = await response.json()
                return data.get("items", [])
            else:
                logging.error(f"Ошибка получения списка товаров: {response.status()} - {response_text}")
                return []
    except aiohttp.ClientError as e:
        logging.error(f"Ошибка сети при запросе товаров Ozon: {e}")
        return []


async def get_ozon_products(session):
    """ Получает данные о товарах с Ozon по списку артикулов (offer_id) """
    url = "https://api-seller.ozon.ru/v3/product/list"
    headers = {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }
    payload = {
        "filter": {
            "visibility": "ALL"
        },
        "last_id": "",
        "limit": 1000
    }

    try:
        async with session.post(url, headers=headers, json=payload) as response:
            if response.status == 200:
                data = await response.json()
                return data.get("result", {}).get("items", [])
            else:
                logging.error(f"Ошибка получения списка товаров: {response.status}")
                return []
    except aiohttp.ClientError as e:
        logging.error(f"Ошибка сети при запросе товаров Ozon: {e}")
        return []


# Обновление пакета товаров на Ozon
async def update_ozon_batch(session, price_data, stock_data):
    price_url = "https://api-seller.ozon.ru/v1/product/import/prices"
    stock_url = "https://api-seller.ozon.ru/v2/products/stocks"
    headers = {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }

    try:
        price_task = session.post(price_url, headers=headers, json=price_data)
        stock_task = session.post(stock_url, headers=headers, json=stock_data)
        price_response, stock_response = await asyncio.gather(price_task, stock_task)

    # Получаем статус ответа
        price_status = price_response.status
        stock_status = stock_response.status

        # Получаем текст ответа
        price_text = await price_response.text()  # Для текстового ответа
        stock_text = await stock_response.text()  # Для текстового ответа

        # Для JSON-ответов
        try:
            price_json = await price_response.json()
        except Exception as e:
            price_json = f"Ошибка при попытке обработать как JSON: {e}"

        try:
            stock_json = await stock_response.json()
        except Exception as e:
            stock_json = f"Ошибка при попытке обработать как JSON: {e}"

        # Если ответы успешны, возвращаем True
        if price_status == 200 and stock_status == 200:
            return True
        else:
            logging.error(f"Ошибка при обновлении товаров. Статус цен: {price_status}, Статус остатков: {stock_status}")
            return False

        return True

    except aiohttp.ClientError as e:
        # Обработка ошибок сети и других ошибок
        logging.error(f"Ошибка при отправке запросов: {e}")
        return False
    except Exception as e:
        # Логируем неизвестные ошибки
        logging.error(f"Неизвестная ошибка: {str(e)}")
        return False

if __name__ == "__main__":
    dp.run_polling(bot)