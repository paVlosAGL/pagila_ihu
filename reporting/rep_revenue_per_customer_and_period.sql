with revenue as (
  select * 
  from turing-position-480316-e6.staging_db.stg_payment
)

, customers as (
  select *
  from turing-position-480316-e6.staging_db.stg_customer
)  

, reporting_dates as (
  select *
  from turing-position-480316-e6.reporting_db.reporting_periods_table
  where reporting_period in ('Day','Month','Year')
)

, revenue_per_period as (
  select
    'Day' as reporting_period
    , Date(revenue.payment_date) as reporting_date
    , customers.customer_id
    , Sum(payment_amount) as total_revenue
  from revenue
  left join customers
   on revenue.customer_id = customers.customer_id
  group by 1,2,3
  union all
  select
    'Month' as reporting_period
    , DATE_TRUNC(date(revenue.payment_date),month) as reporting_period
    , customers.customer_id
    , Sum(revenue.payment_amount)
  from revenue
  left join customers
   on revenue.customer_id = customers.customer_id
  group by 1,2,3
  union all
  select
    'Year' as reporting_period
    , DATE_TRUNC(date(revenue.payment_date),year) as reporting_date
    , customers.customer_id
    , Sum(revenue.payment_amount)
  from revenue
  left join customers
   on revenue.customer_id = customers.customer_id
  group by 1,2,3
)

, final as (
  select
    reporting_dates.reporting_date
    , reporting_dates.reporting_period
    , revenue_per_period.customer_id
    , revenue_per_period.total_revenue as total_revenue
 from reporting_dates
 inner join revenue_per_period
  on reporting_dates.reporting_date = revenue_per_period.reporting_date
  and  reporting_dates.reporting_period = revenue_per_period.reporting_period
 where reporting_dates.reporting_period = 'Day'
 union all
 select
    reporting_dates.reporting_date
    , reporting_dates.reporting_period
    , revenue_per_period.customer_id
    , revenue_per_period.total_revenue as total_revenue
 from reporting_dates
 inner join revenue_per_period
  on reporting_dates.reporting_date = revenue_per_period.reporting_date
  and  reporting_dates.reporting_period = revenue_per_period.reporting_period
 where reporting_dates.reporting_period = 'Month'
 union all 
 select
 
    reporting_dates.reporting_date
    , reporting_dates.reporting_period
    , revenue_per_period.customer_id
    , revenue_per_period.total_revenue as total_revenue
 from reporting_dates
 inner join revenue_per_period
  on reporting_dates.reporting_date = revenue_per_period.reporting_date
  and  reporting_dates.reporting_period = revenue_per_period.reporting_period
 where reporting_dates.reporting_period = 'Year'
)


select * from final limit 50

