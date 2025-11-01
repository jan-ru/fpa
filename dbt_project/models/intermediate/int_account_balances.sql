{{
  config(
    materialized='ephemeral',
    description='Intermediate model calculating account balances and aggregations'
  )
}}

-- Calculate running balances and aggregations for each account
with account_transactions as (
  select
    account_code,
    account_name,
    transaction_date,
    transaction_year,
    transaction_quarter,
    transaction_month,
    booking_number,
    description,
    debit_amount,
    credit_amount,
    net_amount,
    balance_amount,
    vat_amount,
    transaction_type,
    source_file,
    loaded_timestamp
  from {{ ref('stg_financial_transactions') }}
),

account_aggregates as (
  select
    account_code,
    account_name,
    
    -- Transaction counts
    count(*) as total_transactions,
    count(distinct transaction_date) as unique_transaction_dates,
    
    -- Amount aggregations
    sum(debit_amount) as total_debit,
    sum(credit_amount) as total_credit,
    sum(net_amount) as net_balance,
    sum(vat_amount) as total_vat,
    
    -- Date ranges
    min(transaction_date) as first_transaction_date,
    max(transaction_date) as last_transaction_date,
    
    -- Calculate account activity metrics
    case 
      when sum(debit_amount) > sum(credit_amount) then 'Net Debit'
      when sum(credit_amount) > sum(debit_amount) then 'Net Credit'
      else 'Balanced'
    end as account_balance_type,
    
    -- Activity indicators
    case
      when max(transaction_date) >= current_date - 30 then 'Active'
      when max(transaction_date) >= current_date - 90 then 'Recently Active'
      else 'Inactive'
    end as activity_status

  from account_transactions
  group by account_code, account_name
),

account_trends as (
  select
    account_code,
    
    -- Yearly trends
    count(distinct transaction_year) as years_active,
    
    -- Monthly trends (last 12 months)
    sum(case when transaction_date >= current_date - 365 then net_amount else 0 end) as net_amount_12m,
    count(case when transaction_date >= current_date - 365 then 1 end) as transactions_12m,
    
    -- Quarterly trends (last 4 quarters)
    sum(case when transaction_date >= current_date - 270 then net_amount else 0 end) as net_amount_4q
    
  from account_transactions
  group by account_code
)

select
  a.*,
  t.years_active,
  t.net_amount_12m,
  t.transactions_12m,
  t.net_amount_4q,
  
  -- Calculate transaction frequency
  case
    when a.total_transactions = 0 then 0.0
    else cast(a.total_transactions as decimal(10,2)) / 
         greatest(1, (a.last_transaction_date - a.first_transaction_date + 1))
  end as avg_daily_transaction_frequency

from account_aggregates a
left join account_trends t on a.account_code = t.account_code