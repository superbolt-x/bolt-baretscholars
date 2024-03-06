{{ config (
    alias = target.database + '_unbounce_daily_performance'
)}}

SELECT DATE_TRUNC('day',date::date) as dg, 'day' as date_granularity, 
  COALESCE(SUM(daily_visitors),0) as visitors,
  COALESCE(SUM(daily_visits),0) as visits,
  COALESCE(SUM(daily_conversions),0) as conversions,
  COALESCE(SUM(daily_clicks),0) as clicks,
  COALESCE(SUM(daily_form_submits),0) as form_submits
FROM {{ source('gsheet_raw','unbounce_daily_performance') }}
WHERE _fivetran_synced = (SELECT max(_fivetran_synced) FROM {{ source('gsheet_raw','unbounce_daily_performance') }})
GROUP BY 1,2

UNION ALL

SELECT DATE_TRUNC('week',date::date) as dg, 'week' as date_granularity, 
  COALESCE(SUM(daily_visitors),0) as visitors,
  COALESCE(SUM(daily_visits),0) as visits,
  COALESCE(SUM(daily_conversions),0) as conversions,
  COALESCE(SUM(daily_clicks),0) as clicks,
  COALESCE(SUM(daily_form_submits),0) as form_submits
FROM {{ source('gsheet_raw','unbounce_daily_performance') }}
WHERE _fivetran_synced = (SELECT max(_fivetran_synced) FROM {{ source('gsheet_raw','unbounce_daily_performance') }})
GROUP BY 1,2

UNION ALL

SELECT DATE_TRUNC('month',date::date) as dg, 'month' as date_granularity, 
  COALESCE(SUM(daily_visitors),0) as visitors,
  COALESCE(SUM(daily_visits),0) as visits,
  COALESCE(SUM(daily_conversions),0) as conversions,
  COALESCE(SUM(daily_clicks),0) as clicks,
  COALESCE(SUM(daily_form_submits),0) as form_submits
FROM {{ source('gsheet_raw','unbounce_daily_performance') }}
WHERE _fivetran_synced = (SELECT max(_fivetran_synced) FROM {{ source('gsheet_raw','unbounce_daily_performance') }})
GROUP BY 1,2

UNION ALL

SELECT DATE_TRUNC('quarter',date::date) as dg, 'quarter' as date_granularity, 
  COALESCE(SUM(daily_visitors),0) as visitors,
  COALESCE(SUM(daily_visits),0) as visits,
  COALESCE(SUM(daily_conversions),0) as conversions,
  COALESCE(SUM(daily_clicks),0) as clicks,
  COALESCE(SUM(daily_form_submits),0) as form_submits
FROM {{ source('gsheet_raw','unbounce_daily_performance') }}
WHERE _fivetran_synced = (SELECT max(_fivetran_synced) FROM {{ source('gsheet_raw','unbounce_daily_performance') }})
GROUP BY 1,2

UNION ALL

SELECT DATE_TRUNC('year',date::date) as dg, 'year' as date_granularity, 
  COALESCE(SUM(daily_visitors),0) as visitors,
  COALESCE(SUM(daily_visits),0) as visits,
  COALESCE(SUM(daily_conversions),0) as conversions,
  COALESCE(SUM(daily_clicks),0) as clicks,
  COALESCE(SUM(daily_form_submits),0) as form_submits
FROM {{ source('gsheet_raw','unbounce_daily_performance') }}
WHERE _fivetran_synced = (SELECT max(_fivetran_synced) FROM {{ source('gsheet_raw','unbounce_daily_performance') }})
GROUP BY 1,2
