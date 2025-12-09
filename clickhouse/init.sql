CREATE DATABASE IF NOT EXISTS weather_db;
USE weather_db;

-- Таблица для почасовых данных
CREATE TABLE IF NOT EXISTS weather_hourly (
    city String,
    timestamp DateTime,
    temperature Float64,
    precipitation Float64,
    wind_speed Float64,
    wind_direction Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (city, timestamp);

-- Таблица для дневных агрегированных данных
CREATE TABLE IF NOT EXISTS weather_daily (
    city String,
    date Date,
    min_temp Float64,
    max_temp Float64,
    avg_temp Float64,
    total_precipitation Float64,
    max_wind_speed Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (city, date);