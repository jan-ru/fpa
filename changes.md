# Financial Data Platform

Changes in version 0.0.2

## Input page

Prefiously we had an input page that listed the xlsx files in the data directory. We still need that page.

## Dashboard page

Can you merge the Dashboard and Data tabs where the number of accounts and the number of transactions, the balances and the top accounts apply the to transactions selected, and not to the full set.

## Data page

In the transactions tab I want to see a list of the accounts in an ag-grid format.
In the transactions tab I want to see a list of the transactions in an ag-grid format.
In the transactions tab the width of a quarter is the same as the width of 3 months.
We don's need the labels: years, quarters, months. The buttons are self explanatory.


# Analytics page

Here I want to use lightdash. Can you insert a sample screen?
The planned futures for now are:
- Balance sheet
- Income statement
- Cash flow statement
- Metrics over time

Can you show a sample plotly grid on this page?

# Lineage page

Add dbt docs (Recommended)

uv run dbt docs generate
uv run dbt docs serve
- Native dbt lineage visualization
- Zero additional infrastructure
- Shows model dependencies, column lineage, and descriptions
- Perfect for your current complexity level

# Admn page

No changes for now.