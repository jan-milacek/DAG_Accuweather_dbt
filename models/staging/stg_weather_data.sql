{{ config(materialized='view') }}

with source_data as (
    select 
        id,
        observation_date,
        temperature,
        condition,
        fetch_date,
        created_at
    from {{ source('weather_db', 'historical_weather_data') }}
),

cleaned_data as (
    select 
        id,
        observation_date::timestamp as observation_timestamp,
        temperature::numeric(5,2) as temperature_celsius,
        trim(condition) as weather_condition,
        fetch_date::date as data_date,
        created_at::timestamp as record_created_at,
        
        -- Add useful derived fields
        extract(hour from observation_date) as observation_hour,
        extract(dow from fetch_date) as day_of_week,  -- 0=Sunday, 6=Saturday
        
        -- Temperature categories for business analysis
        case 
            when temperature < 0 then 'freezing'
            when temperature between 0 and 10 then 'cold'
            when temperature between 10 and 20 then 'mild'
            when temperature between 20 and 30 then 'warm'
            else 'hot'
        end as temperature_category
        
    from source_data
    where 
        -- Basic data quality filters
        temperature is not null
        and observation_date is not null
        and fetch_date is not null
        -- Temperature sanity check
        and temperature between {{ var('min_temp') }} and {{ var('max_temp') }}
)

select * from cleaned_data
