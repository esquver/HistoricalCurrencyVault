-- Создание схемы для финансовых данных
CREATE SCHEMA IF NOT EXISTS finance;

-- Создание таблицы для хранения информации о валютах
CREATE TABLE IF NOT EXISTS finance.currencies (
    digital_code INTEGER PRIMARY KEY,
    letter_code VARCHAR(3) NOT NULL,
    currency VARCHAR(255) NOT NULL,
    measurement INTEGER NOT NULL
);

-- Создание таблицы для хранения информации о курсах валют
CREATE TABLE IF NOT EXISTS finance.exchange_rates (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    digital_code INTEGER NOT NULL,
    rate NUMERIC(10, 4) NOT NULL,
    CONSTRAINT unique_date_digital_code UNIQUE (date, digital_code),
    FOREIGN KEY (digital_code) REFERENCES finance.currencies(digital_code)
);