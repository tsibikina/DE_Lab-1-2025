import os
import json
import asyncio
import io  # <-- ИСПРАВЛЕНО: Добавлен правильный импорт
from datetime import datetime, timedelta, date

import httpx
from prefect import flow, task
from prefect.blocks.system import Secret
from minio import Minio
from clickhouse_connect import get_client

# --- Конфигурация ---
CITIES = {
    "Москва": {"latitude": 55.7558, "longitude": 37.6173},
    "Самара": {"latitude": 53.1955, "longitude": 50.1018},
}

WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"

# --- Задачи (Tasks) ---

@task(name="Извлечение данных о погоде", retries=3, retry_delay_seconds=60)
async def extract_weather(city: str, lat: float, lon: float) -> dict:
    """Получает прогноз погоды с Open-Meteo API."""
    tomorrow_date = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,windspeed_10m,winddirection_10m",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
        "timezone": "Europe/Moscow",
        "start_date": tomorrow_date,
        "end_date": tomorrow_date,
    }
    async with httpx.AsyncClient() as client:
        response = await client.get(WEATHER_API_URL, params=params)
        response.raise_for_status()
        print(f"Данные для {city} успешно получены.")
        return response.json()

@task(name="Сохранение сырых данных в MinIO")
def save_raw_to_minio(data: dict, city: str, dt: date):
    """Сохраняет сырой JSON-ответ в MinIO."""
    # ИСПРАВЛЕНО: Используем localhost для подключения с хост-машины
    minio_client = Minio(
        "localhost:9002",  # <-- ИЗМЕНЕНО
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
        secure=False
    )
    bucket_name = "weather-raw"
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    
    object_name = f"weather/{city}/{dt.isoformat()}.json"
    json_data = json.dumps(data, indent=2).encode('utf-8')
    
    # ИСПРАВЛЕНО: Используем io.BytesIO вместо asyncio.BytesIO
    minio_client.put_object(
        bucket_name,
        object_name,
        data=io.BytesIO(json_data), # <-- ИЗМЕНЕНО
        length=len(json_data),
        content_type="application/json"
    )
    print(f"Сырые данные для {city} сохранены в MinIO: {object_name}")
    return f"s3://{bucket_name}/{object_name}"

@task(name="Трансформация почасовых данных")
def transform_hourly(data: dict, city: str) -> list[tuple]:
    """Нормализует почасовые данные для загрузки в ClickHouse."""
    hourly_data = data.get("hourly", {})
    times = hourly_data.get("time", [])
    temps = hourly_data.get("temperature_2m", [])
    prec = hourly_data.get("precipitation", [])
    wind_sp = hourly_data.get("windspeed_10m", [])
    wind_dir = hourly_data.get("winddirection_10m", [])

    transformed = []
    for i in range(len(times)):
        transformed.append((
            city,
            datetime.strptime(times[i], "%Y-%m-%dT%H:%M"),
            float(temps[i]),
            float(prec[i]),
            float(wind_sp[i]),
            float(wind_dir[i])
        ))
    print(f"Трансформировано {len(transformed)} почасовых записей для {city}.")
    return transformed

@task(name="Трансформация и агрегация дневных данных")
def transform_daily(data: dict, city: str) -> tuple:
    """Агрегирует дневные данные для загрузки в ClickHouse."""
    daily_data = data.get("daily", {})
    dt = datetime.strptime(daily_data.get("time", [None])[0], "%Y-%m-%d").date()
    min_temp = float(daily_data.get("temperature_2m_min", [0])[0])
    max_temp = float(daily_data.get("temperature_2m_max", [0])[0])
    avg_temp = (min_temp + max_temp) / 2.0
    total_prec = float(daily_data.get("precipitation_sum", [0])[0])
    max_wind = float(daily_data.get("windspeed_10m_max", [0])[0])
    
    print(f"Дневные данные для {city} агрегированы.")
    return (city, dt, min_temp, max_temp, avg_temp, total_prec, max_wind)

@task(name="Загрузка данных в ClickHouse")
def load_to_clickhouse(data: list[tuple], table_name: str):
    """Загружает данные в указанную таблицу ClickHouse."""
    # ИСПРАВЛЕНО: Используем localhost для подключения с хост-машины
    client = get_client(host='localhost', port=8123) # <-- ИЗМЕНЕНО
    
    column_names = {
        "weather_hourly": ["city", "timestamp", "temperature", "precipitation", "wind_speed", "wind_direction"],
        "weather_daily": ["city", "date", "min_temp", "max_temp", "avg_temp", "total_precipitation", "max_wind_speed"]
    }
    
    client.insert(table=f'weather_db.{table_name}', data=data, column_names=column_names[table_name])
    print(f"Загружено {len(data)} строк в таблицу {table_name}.")

# ИСПРАВЛЕНО: Сделали задачу асинхронной
@task(name="Отправка уведомления в Telegram")
async def send_telegram_notification(daily_summary: list[tuple]):
    """Отправляет сводку прогноза в Telegram."""
    try:
        # ИСПРАВЛЕНО: Используем await для асинхронной загрузки секрета
        bot_token_secret = await Secret.load("telegram-bot-token")
        chat_id_secret = await Secret.load("telegram-chat-id")
        bot_token = bot_token_secret.get()
        chat_id = chat_id_secret.get()
    except Exception as e:
        print(f"Не удалось загрузить секреты для Telegram: {e}. Уведомление не отправлено.")
        return

    message_lines = ["🌤️ Прогноз погоды на завтра:\n"]
    
    for city, dt, min_t, max_t, _, total_prec, max_wind in daily_summary:
        line = f"📍 {city}: {min_t:.1f}°C ... {max_t:.1f}°C, осадки {total_prec:.1f} мм, ветер до {max_wind:.1f} м/с"
        if max_wind > 15:
            line += " ⚠️ Сильный ветер!"
        if total_prec > 10:
            line += " ⚠️ Сильные осадки!"
        message_lines.append(line)

    message = "\n".join(message_lines)
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}
    
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()
        print("Уведомление в Telegram успешно отправлено.")


# --- Основной поток (Flow) ---

@flow(name="weather_etl", log_prints=True)
def weather_etl_flow():
    """Основной ETL-пайплайн для сбора данных о погоде."""
    tomorrow_date = date.today() + timedelta(days=1)
    
    hourly_data_to_load = []
    daily_data_to_load = []

    for city, coords in CITIES.items():
        # Extract
        raw_data_future = extract_weather.submit(city, coords["latitude"], coords["longitude"])
        
        # Transform
        hourly_future = transform_hourly.submit(raw_data_future, city)
        daily_future = transform_daily.submit(raw_data_future, city)
        
        # Save raw data
        save_raw_to_minio.submit(raw_data_future.result(), city, tomorrow_date)
        
        # Collect data for batch loading
        hourly_data_to_load.extend(hourly_future.result())
        daily_data_to_load.append(daily_future.result())

    # Load
    if hourly_data_to_load:
        load_to_clickhouse.submit(hourly_data_to_load, "weather_hourly")
    if daily_data_to_load:
        load_to_clickhouse.submit(daily_data_to_load, "weather_daily")
        
    # Notify
    send_telegram_notification.submit(daily_data_to_load)

if __name__ == "__main__":
    # Для локального запуска
    weather_etl_flow()