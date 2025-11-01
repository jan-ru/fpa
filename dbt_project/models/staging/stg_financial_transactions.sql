{{
  config(
    materialized='view',
    description='Staging model for financial transactions from Iceberg versioned data'
  )
}}

-- Load the latest consolidated financial transactions data
with latest_version as (
  -- For now, load the consolidated data
  -- In production, this would query the latest Iceberg table
  select * from read_parquet('/Users/jrm/Projects/nicegui/pipelines/data/iceberg/warehouse/financial_transactions_iceberg.parquet')
),

cleaned as (
  select
    -- Administrative codes
    CodeAdministratie,
    NaamAdministratie,
    
    -- Account information
    CodeGrootboekrekening as account_code,
    NaamGrootboekrekening as account_name,
    
    -- Transaction details
    Code as transaction_code,
    Boekingsnummer as booking_number,
    Boekdatum as transaction_date,
    Periode as period,
    Code1,
    Code2,
    Omschrijving as description,
    
    -- Financial amounts (ensure proper decimal handling)
    case 
      when Debet is null then 0.00
      else cast(Debet as decimal(15,2))
    end as debit_amount,
    
    case 
      when Credit is null then 0.00
      else cast(Credit as decimal(15,2))
    end as credit_amount,
    
    case 
      when Saldo is null then 0.00
      else cast(Saldo as decimal(15,2))
    end as balance_amount,
    
    case 
      when Btwbedrag is null then 0.00
      else cast(Btwbedrag as decimal(15,2))
    end as vat_amount,
    
    Btwcode as vat_code,
    Boekingsstatus as booking_status,
    Nummer as number,
    Factuurnummer as invoice_number,
    
    -- Metadata columns
    _loaded_at as loaded_timestamp,
    _source_file as source_file,
    _data_version as data_version,
    
    -- Derived columns
    extract(year from Boekdatum) as transaction_year,
    extract(quarter from Boekdatum) as transaction_quarter,
    extract(month from Boekdatum) as transaction_month,
    
    -- Transaction type classification
    case 
      when Debet > 0 then 'Debit'
      when Credit > 0 then 'Credit'
      else 'Zero'
    end as transaction_type,
    
    -- Net amount (debit - credit)
    coalesce(Debet, 0.00) - coalesce(Credit, 0.00) as net_amount

  from latest_version
  
  -- Data quality filters
  where transaction_date is not null
    and account_code is not null
)

select * from cleaned