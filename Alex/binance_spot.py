import requests
import pyodbc
import logging
import json
import os
import time
from datetime import datetime, timedelta

from Alex.env import MSSQL_CONNECTION_STRING

# Путь к файлу для хранения данных Binance
BINANCE_DATA_FILE = "binance_spot_data.json"

# Получение данных с Binance или из файла
def get_binance_spot_data():
    # Проверяем, существует ли файл и не старше ли он 1 часа
    if os.path.exists(BINANCE_DATA_FILE):
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(BINANCE_DATA_FILE))
        if datetime.now() - file_mod_time < timedelta(hours=1):
            logging.info("Чтение данных из локального файла...")
            with open(BINANCE_DATA_FILE, "r") as file:
                return json.load(file)

    # Если файл отсутствует или старше 1 часа, запрашиваем данные с Binance
    url = "https://api.binance.com/api/v3/ticker/24hr"
    logging.info("Запрос данных с Binance (спотовый рынок)...")
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Сохраняем данные в файл
    with open(BINANCE_DATA_FILE, "w") as file:
        json.dump(data, file)

    return data

# Обновление данных в таблице MSSQL
def update_symbols_table(data):
    try:
        # Подключение к базе данных
        conn = pyodbc.connect(MSSQL_CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute(f"UPDATE dbo.Symbols SET mTrades24hCount=0 where mExchange = 2 and mType = 'Spot'")
        conn.commit()
        for item in data:
            symbol = item['symbol']  # Символ торгуемой пары (например, BTCUSDT)
            trades_count = item['count']  # Количество сделок за последние 24 часа

            # Проверяем, существует ли уже символ в таблице
            query_check = f"SELECT mID FROM dbo.Symbols WHERE Name = '{symbol}' AND mExchange = 2 and mType = 'Spot'"
            cursor.execute(query_check)
            row = cursor.fetchone()

            if row:
                # Если символ существует, обновляем запись
                query_update = f"""
                    UPDATE dbo.Symbols
                    SET mTrades24hCount = {trades_count},
                        mLastTime = GETDATE(),
                        mExchangeStr = 'Binance',
                        mType = 'Spot',
                        mDb = 'binance',
                        mDbSpot = 'binance_spot_deals'
                        
                    WHERE mID = {row.mID}
                """
                cursor.execute(query_update)
                conn.commit()
                logging.info(f"Обновили запись в таблице Symbols (спотовый рынок) для символа {symbol}. mTrades24hCount = {trades_count}")
            else:
                # Если символ не существует, вставляем новую запись
                query_insert = f"""
                    INSERT INTO dbo.Symbols (Name, mStatusTrading, key_Trading_Allowed_Spot, mDbSpot, mExchange, mTrades24hCount, key_Trading_Allowed_Futures, mType, key_Save, mExchangeStr, mLastTime)
                    VALUES ('{symbol}', 1, 1, 'binance_spot_deals', 2, {trades_count}, 0, 'Spot', 0, 'Binance', GETDATE())
                """
                cursor.execute(query_insert)
                conn.commit()
                logging.info(f"Вставили новую строку в таблицу Symbols (спотовый рынок) для символа {symbol}.")

        # Сохраняем изменения
        conn.commit()

    except pyodbc.Error as e:
        logging.error(f"Ошибка при работе с базой данных: {e}")
    finally:
        # Закрываем соединение
        conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    binance_data = get_binance_spot_data()
    update_symbols_table(binance_data)