-- Test to ensure temperatures are within reasonable ranges for Prague
-- This test will fail if any temperatures are outside expected ranges

select 
    data_date,
    avg_temperature,
    min_temperature,
    max_temperature
from {{ ref('daily_weather_summary') }}
where 
    avg_temperature < -30  -- Unreasonably cold for Prague
    or avg_temperature > 45  -- Unreasonably hot for Prague
    or min_temperature > max_temperature  -- Logic error
    or abs(max_temperature - min_temperature) > 40  -- Unrealistic daily range
