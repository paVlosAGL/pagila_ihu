with revenue as (
  select *
  from turing-position-480316-e6.staging_db.stg_payment
)

, reporting_dates as (
  select *
  from turing-position-480316-e6.reporting_db.reporting_periods_table
  where reporting_period in ('Day','Month','Year')
    and reporting_date>='2015-01-01'
)

, revenue_per_period as (
  select 
    'Day' as reporting_period
    , Date(revenue.payment_date) as reporting_date
    , Sum(revenue.payment_amount) as total_revenue
  from revenue
  group by 1,2
union all
  select
    'Month' as reporting_period
    , Date_trunc(date(revenue.payment_date),month) as reporting_date
    , Sum(revenue.payment_amount) as total_revenue
  from revenue
  group by 1,2
union all
  select
    'Year' as reporting_period
    , Date_trunc(date(revenue.payment_date),year) as reporting_date
    , Sum(revenue.payment_amount) as total_revenue
  from revenue
  group by 1,2
)

, final as (
  select
    reporting_dates.reporting_date
    , reporting_dates.reporting_period
    , coalesce(revenue_per_period.total_revenue,0) as total_revenue
  from reporting_dates
  left join revenue_per_period
  on reporting_dates.reporting_period = revenue_per_period.reporting_period
  and reporting_dates.reporting_date = revenue_per_period.reporting_date
  where reporting_dates.reporting_period = 'Day'
  union all
  select
    reporting_dates.reporting_date
    , reporting_dates.reporting_period
    , coalesce(revenue_per_period.total_revenue,0) as total_revenue
  from reporting_dates
  left join revenue_per_period
  on reporting_dates.reporting_date = revenue_per_period.reporting_date
  and reporting_dates.reporting_period = revenue_per_period.reporting_period
  where reporting_dates.reporting_period = 'Month'
  union all
  select
    reporting_dates.reporting_date
    , reporting_dates.reporting_period
    , coalesce(revenue_per_period.total_revenue,0) as total_revenue
  from reporting_dates
  left join revenue_per_period
  on reporting_dates.reporting_date = revenue_per_period.reporting_date
  and reporting_dates.reporting_period = revenue_per_period.reporting_period
  where reporting_dates.reporting_period = 'Year'
)

select * from final











