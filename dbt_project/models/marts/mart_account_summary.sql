{{
  config(
    materialized='table',
    description='Account summary mart - final table for account analytics and reporting'
  )
}}

-- Final account summary for consumption by applications
select
  account_code,
  account_name,
  
  -- Transaction metrics
  total_transactions,
  unique_transaction_dates,
  avg_daily_transaction_frequency,
  
  -- Financial metrics (rounded for display)
  round(total_debit, 2) as total_debit,
  round(total_credit, 2) as total_credit,
  round(net_balance, 2) as net_balance,
  round(total_vat, 2) as total_vat,
  
  -- Recent activity metrics
  round(net_amount_12m, 2) as net_amount_last_12_months,
  transactions_12m as transactions_last_12_months,
  round(net_amount_4q, 2) as net_amount_last_4_quarters,
  
  -- Date information
  first_transaction_date,
  last_transaction_date,
  years_active,
  
  -- Classifications
  account_balance_type,
  activity_status,
  
  -- Additional calculated fields
  case
    when total_transactions > 100 then 'High Volume'
    when total_transactions > 20 then 'Medium Volume'
    else 'Low Volume'
  end as transaction_volume_category,
  
  case
    when abs(net_balance) > 10000 then 'High Value'
    when abs(net_balance) > 1000 then 'Medium Value'
    else 'Low Value'
  end as balance_value_category,
  
  -- Data lineage
  current_timestamp as last_updated

from {{ ref('int_account_balances') }}

-- Only include accounts with actual transactions
where total_transactions > 0

order by abs(net_balance) desc