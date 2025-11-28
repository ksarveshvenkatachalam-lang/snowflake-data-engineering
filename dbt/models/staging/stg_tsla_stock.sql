-- Staging model for Tesla (TSLA) stock data
-- Cleans and standardizes raw stock price data

{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_tsla_stock') }}
),

cleaned AS (
    SELECT
        date AS trading_date,
        ROUND(open, 2) AS open_price,
        ROUND(high, 2) AS high_price,
        ROUND(low, 2) AS low_price,
        ROUND(close, 2) AS close_price,
        ROUND(adj_close, 2) AS adjusted_close,
        volume AS trading_volume,
        
        -- Derived fields
        ROUND(high - low, 2) AS daily_range,
        ROUND(close - open, 2) AS daily_change,
        ROUND(((close - open) / NULLIF(open, 0)) * 100, 2) AS daily_return_pct,
        
        -- Metadata
        _loaded_at
    FROM source
    WHERE date IS NOT NULL
      AND close > 0
)

SELECT * FROM cleaned
