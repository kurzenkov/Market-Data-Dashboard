# Market-Data-Dashboard
 Market-Data-Dashboard - этот проект состоит из трех скриптов. Получает данные по api от:
1) binance
2) Bybit
3) OKX
 
Main.py он обращается к биржам и через апи получается спотовые и фьючерсные данные
check_data - поднимает локальный сервер и отображает данные + добавлены функции сортировки
binance_module - он берет данные опционов с трех бирж и сохраняет их в той же базе данных

Порядок запуска:
1) Main.py
2) binance_module
3) check_data
   

В папке с проектом идет файл requirements с основными переменными среды
