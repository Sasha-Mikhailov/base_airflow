# Airflow + ExchangeRates + PostgreSQL
**I.** скопировать репозиторий 
```bash
git clone https://github.com/Sasha-Mikhailov/base_airflow.git
cd base_airflow
```

**II.** указать коннект для внутренней базу для Airflow: оставить стандартную (из докер-контейнера рядом) или указать conn_string для своей. 
Указывается в [docker-compose.yaml](https://github.com/Sasha-Mikhailov/base_airflow/blob/d479f34a2d2a80d656eba56df667d00714c2684b/docker-compose.yaml#L52-L53) 

**III.** Запустить «ансамбль» докер-контейнеров, подождать запуска (и инициилизации, если первый запуск):
```bash
docker-compose up
```

**IV.** Должен заработать webserver для Airflow. Зайти на http://localhost:8080 с логином-паролем `airflow`

<img width="1184" alt="image" src="https://user-images.githubusercontent.com/51232538/154117829-1f87b1d4-02a9-49f4-98a2-d042941baef4.png">


**V.** запустить `DAG'и`:
1. **get_current_rate** — регулярный запуск, запрашивающий текущий курс
2. **get_historical_rates** - исторический пересчёт за конкретный период

<img width="1175" alt="image" src="https://user-images.githubusercontent.com/51232538/154117884-f4812efe-3a8f-4031-9441-b2173ad34b3b.png">


### Таски
Регулярный запуск запрашивает текущий курс и пишет результат в базу.

Исторический пересчёт принимает на вход параметры через Airflow Variables:
- дату начала перода
- окончание периода
- код валюты из
- код валюты в

<img width="551" alt="image" src="https://user-images.githubusercontent.com/51232538/154117713-40640aef-420e-4ab9-a5a0-60b2c914b79b.png">


при записи данных в базу предварительно удаляет строки за тот же период (в целях идемпотентности).

Проверить данные в базе — подключиться по коннектам, указанным в п. II.


### Метаданные 
Метаданные для таблицы храняться тут же в коде [dags/common/meta.py](dags/common/meta.py). При первом запуске проверяёт наличие таблицы и создаёт, если не находит такой
