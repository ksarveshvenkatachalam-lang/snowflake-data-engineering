-- Analytics Mart: TSLA Daily Summary
-- Final reporting table with all metrics for dashboards and analysis

{{ config(
    materialized='table',
    schema='analytics'
) }}

WITH technical AS (
    SELECT * FROM {{ ref('int_tsla_technical_indicators') }}
),

final AS (
    SELECT
        -- Date dimensions
        trading_date,
        YEAR(trading_date) AS year,
        QUARTER(trading_date) AS quarter,
        MONTH(trading_date) AS month,
        DAYOFWEEK(trading_date) AS day_of_week,
        DAYNAME(trading_date) AS day_name,
        
        -- Price metrics
        open_price,
        high_price,
        low_price,
        close_price,
        adjusted_close,
        daily_range,
        daily_change,
        daily_return_pct,
        
        -- Volume
        trading_volume,
        avg_volume_20d,
        CASE 
            WHEN trading_volume > avg_volume_20d * 1.5 THEN 'HIGH'
            WHEN trading_volume < avg_volume_20d * 0.5 THEN 'LOW'
            ELSE 'NORMAL'
        END AS volume_category,
        
        -- Moving averages
        ma_7d,
        ma_20d,
        ma_50d,
        ma_200d,
        
        -- Trend signals
        ma_50d_signal,
        trend_signal,
        
        -- Momentum
        momentum_5d_pct,
        momentum_20d_pct,
        CASE
            WHEN momentum_20d_pct > 10 THEN 'STRONG_UP'
            WHEN momentum_20d_pct > 0 THEN 'UP'
            WHEN momentum_20d_pct > -10 THEN 'DOWN'
            ELSE 'STRONG_DOWN'
        END AS momentum_category,
        
        -- Volatility
        volatility_20d,
        CASE
            WHEN volatility_20d > 50 THEN 'VERY_HIGH'
            WHEN volatility_20d > 20 THEN 'HIGH'
            WHEN volatility_20d > 10 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS volatility_category,
        
        -- 52-week range
        high_52w,
        low_52w,
        position_in_52w_range_pct,
        
        -- Classification
        CASE
            WHEN daily_return_pct >= 5 THEN 'BIG_GAIN'
            WHEN daily_return_pct >= 2 THEN 'GAIN'
            WHEN daily_return_pct >= -2 THEN 'FLAT'
            WHEN daily_return_pct >= -5 THEN 'LOSS'
            ELSE 'BIG_LOSS'
        END AS daily_performance
        
    FROM technical
)

SELECT * FROM final
ORDER BY trading_date DESC
