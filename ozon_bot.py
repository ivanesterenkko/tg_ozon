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
    file_path = '/home/alex/tg_ozon/ozon_artikules.xlsx'
    pdf = pd.read_excel(file_path)
    article_data = pdf.iloc[:, 0].dropna().astype(str).str.strip().values
    updated_products = []

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    connector = TCPConnector(ssl=ssl_context)

    async with aiohttp.ClientSession(connector=connector) as session:
        batch_size = 100
        rows = list(df.iterrows())
        logging.info(len(rows))
        categories_id = ['А   Дренажные насосы', 'Б   Колодезные насосы', 'В   Фекальные насосы', 'Г   Скважинные насосы', 'Д   Самовсасывающие насосы', 'Е   Вихревые насосы', 'Ж   Центробежные', 'З   Многоступенчатые', 'К   Нас. авт.станц', 'Л   Нас. авт.станц. с защ. с/сх', 'М   Баки', 'Н   Аксессуары', 'О   Пульты', 'П   Станции управления', 'Р   Комбипрессы', 'С   Установка SAR', '*   Гидравлика', '*   Электродвигатели', 'Артикул', 'Итого']
        kgt = ['48SGD9812A', '48SGD9813A', '48SGD9814A', '48SGD9812A1', '48SGM970CA', '48SGM970GA', '48SGM970DA', '48SGM970EA', '48SGM970IA', '48SGM9851A', '48SGM9852A', '48SGM9853A', '48SGM9863A', '48SGM9854A', '48SGM9864A', '48SGQ9851A', '48SGQ9863A', '48SGQ9854A', '48SGQ9864A', '48SGM9852A1', '48SGQ9852A1', '48SGQ9853A1', '48SHT02A', '48SHT03A', '48SHT9803A', '48SHT07A', '48SHT9804A', '48SHT08A', '48SGV970GA', '48SGV970PA', '48SGV970HA', '48SGV970MA', '48SGV970QA', '48SGV970NA', '48SGV970RA', '48SGV9851A', '48SGV9852A', '48SGV9863A', '48SGV9854A', '48SGV9864A', '48SGVP970EA', '48SGY9851A', '48SGY9861A', '48SGY9852A', '48SGY9862A', '48SGY9854A', '48SGY9864A', '48SGV9861A1', '48SGV9852A1', '48SGV9853A1', '48SGY9852A1', '48SGY9853A1', '48SGY947AA1', '4941222WLA', '496H3427WLA', '496B1208A', '496B1211A', '496B1215A', '496B1218A', '496B1221A', '496B1225A', '496B1809A', '496B1811A', '496B1813A', '496B1815A', '496B1818A', '496B1822A', '496B2705A', '496B2712A', '496B2727A', '49623604WLA', '49624404WLA', '46JDNP7A30A1', '46JS8AH15A', '46JS8AM15A', '46JS8AL10A', '46JS8AL05A', '46JS8AM05A', '46JS8AL15A1', '46JS8AL05A1', '46JS8AH05A1', '43PJD15038A', '43PJC20048A', '43PJB30068A', '43PJC15038A1', '43PJD15038A1', '41PM9217A1', '43FCR0152A', '43FCR0302A', '452CT313BA', '452CT303CA', '452CT353AE', '452CT343BE', '452CT403AE', '452CT383CA', '452CT420AE', '452CT420BE', '452CM2616AA1', '452CM2616BA1', '452CM2614CA1', '44CT216AA', '44CT217HAE', '44CT217AE', '44CT217BA', '44CP250ANE', '44CM26BA1', '44CM26CA1', '44CI175A1', '44CI17MA1', '44CI19A1', '4FN32203AE', '4FN3220HAA', '4FN32203BE', '4FN3220HBA', '4FN32203CA', '4FN32250AE', '4FN32250BE', '4FN32250CE', '4FN40163AA', '4FN40159B1A', '4FN40203AE', '4FN40203BE', '4FN40250AE', '4FN40250BE', '4FN40250CE', '4FN50161AA', '4FN50162B1A', '4FN50163AE', '4F50163XAE', '4FN50163BE', '4F50163XBE', '4FN50163CA', '4FN50165AE', '4FN50165BE', '4FN50165CE', '4FN50170E', '4FN50172AE', '4FN50169E', '4FN50167E', '4FN65125AE', '4F65125XAE', '4FN65125BE', '4F65125XBE', '4FN65125CA', '4F65125XCA', '4FN65160E', '4FN65159E', '4FN65158E', '4FN65165AE', '4FN65170AE', '4FN65165BE', '4FN80160AE', '4FN80160BE', '4FN80160CE', '4FN80160DE', '4FN80200BE', '4FNA10160BNE', '4FNA10200BE', '4FP50163BA', '4FP65125BA', '4FP80160DA', '47HF83T0AA', '47HF8T0B1A', '47HF826AA', '47HF826BA', '47HF93TAE', '47HF93TBE', '48HT00307A1']
        updated_products = []
        k = 0
        e = 0
        logging.info(len(article_data))
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