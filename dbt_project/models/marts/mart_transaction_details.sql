{{
  config(
    materialized='table',
    description='Transaction details mart - final table for transaction-level analysis'
  )
}}

-- Final transaction details for consumption by applications
with enriched_transactions as (
  select
    -- Primary transaction details
    account_code,
    account_name,
    transaction_date,
    booking_number,
    description,
    
    -- Financial details
    round(debit_amount, 2) as debit_amount,
    round(credit_amount, 2) as credit_amount,
    round(net_amount, 2) as net_amount,
    round(balance_amount, 2) as balance_amount,
    round(vat_amount, 2) as vat_amount,
    
    -- Transaction classification
    transaction_type,
    transaction_year,
    transaction_quarter,
    transaction_month,
    
    -- Additional codes and references
    transaction_code,
    Code1,
    Code2,
    vat_code,
    booking_status,
    number,
    invoice_number,
    
    -- Metadata
    source_file,
    loaded_timestamp,
    
    -- Date-based enrichment
    case extract(dow from transaction_date)
      when 0 then 'Sunday'
      when 1 then 'Monday' 
      when 2 then 'Tuesday'
      when 3 then 'Wednesday'
      when 4 then 'Thursday'
      when 5 then 'Friday'
      when 6 then 'Saturday'
    end as day_of_week,
    
    extract(day from transaction_date) as day_of_month,
    
    case 
      when extract(month from transaction_date) in (1,2,3) then 'Q1'
      when extract(month from transaction_date) in (4,5,6) then 'Q2'
      when extract(month from transaction_date) in (7,8,9) then 'Q3'
      when extract(month from transaction_date) in (10,11,12) then 'Q4'
    end as quarter_label,
    
    -- Amount categorization
    case
      when abs(net_amount) > 5000 then 'Large'
      when abs(net_amount) > 1000 then 'Medium'
      when abs(net_amount) > 100 then 'Small'
      else 'Minimal'
    end as amount_category,
    
    -- Recency classification
    case
      when transaction_date >= current_date - 30 then 'Last 30 days'
      when transaction_date >= current_date - 90 then 'Last 90 days'
      when transaction_date >= current_date - 365 then 'Last year'
      else 'Older than 1 year'
    end as recency_category

  from {{ ref('stg_financial_transactions') }}
)

select 
  -- Create a unique row identifier using hash with more fields
  hash(concat(account_code, '|', transaction_date, '|', booking_number, '|', description, '|', net_amount)) as transaction_id,
  
  *,
  
  -- Add running totals by account (for balance tracking)
  sum(net_amount) over (
    partition by account_code 
    order by transaction_date, booking_number 
    rows unbounded preceding
  ) as running_balance,
  
  -- Data quality indicators
  case
    when debit_amount = 0 and credit_amount = 0 then 'Zero Amount'
    when debit_amount > 0 and credit_amount > 0 then 'Both Debit and Credit'
    when description is null or trim(description) = '' then 'Missing Description'
    else 'Complete'
  end as data_quality_flag,
  
  -- Final update timestamp
  current_timestamp as last_updated

from enriched_transactions

order by transaction_date desc, booking_number desc