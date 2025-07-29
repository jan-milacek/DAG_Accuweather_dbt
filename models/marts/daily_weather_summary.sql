{{ config(materialized='table') }}

with daily_aggregations as (
    select 
        data_date,
        
        -- Temperature metrics
        min(temperature_celsius) as min_temperature,
        max(temperature_celsius) as max_temperature,
        round(avg(temperature_celsius), 2) as avg_temperature,
        
        -- Temperature range and variability
        max(temperature_celsius) - min(temperature_celsius) as temperature_range,
        round(stddev(temperature_celsius), 2) as temperature_std_dev,
        
        -- Observation quality
        count(*) as total_observations,
        count(distinct observation_hour) as unique_hours_observed,
        
        -- Weather conditions (most frequent)
        mode() within group (order by weather_condition) as predominant_condition,
        count(distinct weather_condition) as condition_changes,
        
        -- Temperature category distribution
        sum(case when temperature_category = 'freezing' then 1 else 0 end) as freezing_hours,
        sum(case when temperature_category = 'cold' then 1 else 0 end) as cold_hours,
        sum(case when temperature_category = 'mild' then 1 else 0 end) as mild_hours,
        sum(case when temperature_category = 'warm' then 1 else 0 end) as warm_hours,
        sum(case when temperature_category = 'hot' then 1 else 0 end) as hot_hours,
        
        -- Day characteristics
        day_of_week,
        case 
            when day_of_week in (0, 6) then 'weekend'
            else 'weekday'
        end as day_type
        
    from {{ ref('stg_weather_data') }}
    group by data_date, day_of_week
),

enriched_daily as (
    select 
        *,
        
        -- Business-friendly temperature assessment
        case 
            when avg_temperature < 5 then 'very_cold'
            when avg_temperature between 5 and 15 then 'cold'
            when avg_temperature between 15 and 25 then 'comfortable'
            when avg_temperature between 25 and 30 then 'warm'
            else 'hot'
        end as daily_temperature_assessment,
        
        -- Data quality flag
        case 
            when unique_hours_observed >= 20 then 'complete'
            when unique_hours_observed >= 12 then 'good'
            else 'incomplete'
        end as data_quality_flag
        
    from daily_aggregations
)

select * from enriched_daily
order by data_date desc
