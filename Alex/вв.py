import clickhouse_connect
import pandas as pd
import webbrowser
import os

from Alex.env import *

# Функция для соединения с ClickHouse
def get_clickhouse_client():

    client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST,
                                           port=CLICKHOUSE_PORT,
                                           username=CLICKHOUSE_USERNAME,
                                           password=CLICKHOUSE_PASSWORD)

    return client


# Функция для поиска таблиц с полем moment и типом Datetime
def find_tables_with_moment(client):
    query = """
    SELECT table, database, name, type
    FROM system.columns
    WHERE name = 'Moment' AND database NOT IN ('system', 'information_schema')
    """
    result = client.query(query)
    return result.result_rows


# Функция для получения количества строк за последний час для каждой таблицы
def get_row_count_last_hour(client, database, table):
    query = f"""
    SELECT count() 
    FROM `{database}`.`{table}`
    WHERE Moment >= now() - INTERVAL 5 HOUR
    """
    result = client.query(query)
    return result.result_rows[0][0]


# Функция для создания HTML-страницы с кастомной пагинацией, сортировкой и фильтрацией
def create_html_page(data):
    df = pd.DataFrame(data, columns=["Database", "Table", "Rows in Last Hour"])
    html_content = df.to_html(index=False)

    # Добавляем DataTables библиотеку и скрипт для активации сортировки, фильтрации и кастомной пагинации
    html_template = f"""
    <html>
    <head>
        <title>ClickHouse Report - Rows in Last Hour</title>
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.css">
        <script type="text/javascript" charset="utf8" src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
        <script type="text/javascript" charset="utf8" src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.js"></script>
    </head>
    <body>
        <h1>ClickHouse Report - Rows in Last Hour</h1>
        {html_content}
        <script>
            $(document).ready(function() {{
                $('table').DataTable({{
                    "paging": true,
                    "lengthMenu": [[-1, 100, 500], ["All", 100, 500]],
                    "pageLength": -1,  // По умолчанию отображаются все записи
                    "searching": true,
                    "ordering": true,
                    "order": [[2, "desc"]],  // Сортировка по столбцу "Rows in Last Hour" (индекс 2) по убыванию
                    "info": true
                }});
            }});
        </script>
    </body>
    </html>
    """

    # Записываем HTML в файл
    html_file = 'clickhouse_report.html'
    with open(html_file, 'w') as f:
        f.write(html_template)

    # Открываем файл в браузере
    webbrowser.open(f'file://{os.path.realpath(html_file)}')


# Основная функция
def main():
    client = get_clickhouse_client()

    # Поиск таблиц с полем moment
    tables = find_tables_with_moment(client)

    data = []
    for table_info in tables:
        table, database, _, _ = table_info
        row_count = get_row_count_last_hour(client, database, table)
        data.append([database, table, row_count])

    # Создаем и открываем HTML-страницу
    create_html_page(data)


if __name__ == "__main__":
    main()
