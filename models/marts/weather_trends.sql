{{ config(materialized='table') }}

with daily_temps as (
    select 
        data_date,
        avg_temperature,
        min_temperature,
        max_temperature,
        predominant_condition,
        data_quality_flag
    from {{ ref('daily_weather_summary') }}
    where data_quality_flag in ('complete', 'good')  -- Only use quality data for trends
),

trend_calculations as (
    select 
        data_date,
        avg_temperature,
        min_temperature,
        max_temperature,
        predominant_condition,
        
        -- 7-day rolling averages
        round(avg(avg_temperature) over (
            order by data_date 
            rows between 6 preceding and current row
        ), 2) as avg_temp_7day,
        
        round(avg(min_temperature) over (
            order by data_date 
            rows between 6 preceding and current row
        ), 2) as min_temp_7day,
        
        round(avg(max_temperature) over (
            order by data_date 
            rows between 6 preceding and current row
        ), 2) as max_temp_7day,
        
        -- Temperature changes (day-over-day)
        lag(avg_temperature, 1) over (order by data_date) as prev_day_avg_temp,
        
        -- Week-over-week comparison
        lag(avg_temperature, 7) over (order by data_date) as same_day_last_week_temp,
        
        -- Row number for trend period validation
        row_number() over (order by data_date) as day_sequence
        
    from daily_temps
),

final_trends as (
    select 
        data_date,
        avg_temperature,
        min_temperature,
        max_temperature,
        predominant_condition,
        
        -- Rolling averages (only show after 7 days of data)
        case when day_sequence >= 7 then avg_temp_7day end as rolling_avg_7day,
        case when day_sequence >= 7 then min_temp_7day end as rolling_min_7day,
        case when day_sequence >= 7 then max_temp_7day end as rolling_max_7day,
        
        -- Daily temperature change
        round(avg_temperature - prev_day_avg_temp, 2) as temp_change_daily,
        
        -- Weekly temperature change
        round(avg_temperature - same_day_last_week_temp, 2) as temp_change_weekly,
        
        -- Trend indicators
        case 
            when avg_temperature - prev_day_avg_temp > 3 then 'warming'
            when avg_temperature - prev_day_avg_temp < -3 then 'cooling'
            else 'stable'
        end as daily_trend,
        
        case 
            when day_sequence >= 7 and avg_temperature > avg_temp_7day + 2 then 'above_average'
            when day_sequence >= 7 and avg_temperature < avg_temp_7day - 2 then 'below_average'
            else 'normal'
        end as trend_vs_recent_average
        
    from trend_calculations
)

select * from final_trends
where data_date >= current_date - interval '90 days'  -- Keep last 90 days for performance
order by data_date desc
