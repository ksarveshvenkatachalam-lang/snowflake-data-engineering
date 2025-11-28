-- ============================================
-- TSLA Stock Data - Raw Table Creation
-- ============================================
-- This script creates the raw table for Tesla stock price data

USE DATABASE DATA_ENGINEERING_DB;
USE SCHEMA RAW;

-- Create raw table for TSLA stock data
CREATE OR REPLACE TABLE RAW_TSLA_STOCK (
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    adj_close FLOAT,
    volume BIGINT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create file format for CSV loading
CREATE OR REPLACE FILE FORMAT tsla_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- Create stage for loading data
CREATE OR REPLACE STAGE tsla_stage
    FILE_FORMAT = tsla_csv_format;

-- Load data command (run after uploading file to stage)
-- PUT file://path/to/TSLA.csv @tsla_stage;
-- COPY INTO RAW_TSLA_STOCK (date, open, high, low, close, adj_close, volume)
-- FROM @tsla_stage/TSLA.csv;
