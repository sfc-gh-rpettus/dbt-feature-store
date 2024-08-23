
select
    TX_DATETIME,
    CUSTOMER_ID,
    TX_AMOUNT,
    {{ rolling_agg('TX_AMOUNT', 'CUSTOMER_ID', 'TX_DATETIME', '1 days', 'sum') }} as TX_AMOUNT_1D,
    {{ rolling_agg('TX_AMOUNT', 'CUSTOMER_ID', 'TX_DATETIME', '7 days', 'sum') }} as TX_AMOUNT_7D,
    {{ rolling_agg('TX_AMOUNT', 'CUSTOMER_ID', 'TX_DATETIME', '30 days', 'sum') }} as TX_AMOUNT_30D,
    {{ rolling_agg('TX_AMOUNT', 'CUSTOMER_ID', 'TX_DATETIME', '1 days', 'avg') }} as TX_AMOUNT_AVG_1D,
    {{ rolling_agg('TX_AMOUNT', 'CUSTOMER_ID', 'TX_DATETIME', '7 days', 'avg') }} as TX_AMOUNT_AVG_7D,
    {{ rolling_agg('TX_AMOUNT', 'CUSTOMER_ID', 'TX_DATETIME', '30 days', 'avg') }} as TX_AMOUNT_AVG_30D,
    {{ rolling_agg('*', 'CUSTOMER_ID', 'TX_DATETIME', '1 days', 'count') }} as TX_CNT_1D,
    {{ rolling_agg('*', 'CUSTOMER_ID', 'TX_DATETIME', '7 days', 'count') }} as TX_CNT_7D,
    {{ rolling_agg('*', 'CUSTOMER_ID', 'TX_DATETIME', '30 days', 'count') }} as TX_CNT_30D
from {{ref("stg_transactions")}}