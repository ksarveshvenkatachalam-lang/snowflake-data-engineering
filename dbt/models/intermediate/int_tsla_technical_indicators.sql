-- Intermediate model: TSLA Technical Indicators
-- Calculates moving averages, volatility, and other technical metrics

{{ config(
    materialized='table',
    schema='intermediate'
) }}

WITH staged AS (
    SELECT * FROM {{ ref('stg_tsla_stock') }}
),

with_moving_averages AS (
    SELECT
        *,
        -- Moving Averages
        AVG(close_price) OVER (ORDER BY trading_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7d,
        AVG(close_price) OVER (ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma_20d,
        AVG(close_price) OVER (ORDER BY trading_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma_50d,
        AVG(close_price) OVER (ORDER BY trading_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma_200d,
        
        -- Volume averages
        AVG(trading_volume) OVER (ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_volume_20d,
        
        -- Price momentum
        LAG(close_price, 1) OVER (ORDER BY trading_date) AS prev_close,
        LAG(close_price, 5) OVER (ORDER BY trading_date) AS close_5d_ago,
        LAG(close_price, 20) OVER (ORDER BY trading_date) AS close_20d_ago,
        
        -- Volatility (20-day rolling std dev)
        STDDEV(close_price) OVER (ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS volatility_20d,
        
        -- High/Low tracking
        MAX(high_price) OVER (ORDER BY trading_date ROWS BETWEEN 51 PRECEDING AND CURRENT ROW) AS high_52w,
        MIN(low_price) OVER (ORDER BY trading_date ROWS BETWEEN 51 PRECEDING AND CURRENT ROW) AS low_52w
    FROM staged
),

final AS (
    SELECT
        trading_date,
        open_price,
        high_price,
        low_price,
        close_price,
        adjusted_close,
        trading_volume,
        daily_range,
        daily_change,
        daily_return_pct,
        
        -- Moving averages (rounded)
        ROUND(ma_7d, 2) AS ma_7d,
        ROUND(ma_20d, 2) AS ma_20d,
        ROUND(ma_50d, 2) AS ma_50d,
        ROUND(ma_200d, 2) AS ma_200d,
        
        -- Signals
        CASE WHEN close_price > ma_50d THEN 'ABOVE' ELSE 'BELOW' END AS ma_50d_signal,
        CASE WHEN ma_50d > ma_200d THEN 'BULLISH' ELSE 'BEARISH' END AS trend_signal,
        
        -- Momentum
        ROUND(((close_price - close_5d_ago) / NULLIF(close_5d_ago, 0)) * 100, 2) AS momentum_5d_pct,
        ROUND(((close_price - close_20d_ago) / NULLIF(close_20d_ago, 0)) * 100, 2) AS momentum_20d_pct,
        
        -- Volatility
        ROUND(volatility_20d, 2) AS volatility_20d,
        ROUND(avg_volume_20d, 0) AS avg_volume_20d,
        
        -- 52-week metrics
        ROUND(high_52w, 2) AS high_52w,
        ROUND(low_52w, 2) AS low_52w,
        ROUND(((close_price - low_52w) / NULLIF(high_52w - low_52w, 0)) * 100, 2) AS position_in_52w_range_pct
        
    FROM with_moving_averages
)

SELECT * FROM final
