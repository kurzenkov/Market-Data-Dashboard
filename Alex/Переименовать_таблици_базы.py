import logging
import clickhouse_connect

from Alex.env import *  # Предполагается, что переменные окружения уже настроены

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def work():
    client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST,
                                           port=CLICKHOUSE_PORT,
                                           username=CLICKHOUSE_USERNAME,
                                           password=CLICKHOUSE_PASSWORD)

    dbname = "binance_spot_quotes"

    # Выполняем запрос
    resp = client.query(f"SHOW TABLES FROM {dbname}")

    # Получаем строки результата
    tables = resp.result_rows

    for obj in tables:
        name = obj[0]  # Предполагаем, что имя таблицы находится в первом столбце
        if name.startswith("q_"):
            sql = f"RENAME TABLE {dbname}.{name} TO `{dbname}`.`{name[2:]}`"
            ret = client.command(sql)  # Используем команду для выполнения SQL
            if not ret:
                logger.error(f"Ошибка при выполнении: {sql}")
                raise Exception(f"Ошибка при выполнении: {sql}")
            else:
                logger.info(f"Таблица {name} переименована в {name[2:]}")


if __name__ == "__main__":
    work()
