import snowflake.snowpark.functions as F
from snowflake.snowpark import Window

def model(dbt, session):
    # Reference the stg_transactions table
    df_trx = dbt.ref('stg_transactions')

    # Get the Date Information for the Dataset (number of days and start_date)
    date_info = df_trx.select(
        F.min(F.col("TX_DATETIME")).as_("END_DATE"),
        F.datediff("DAY", F.col("END_DATE"), F.max(F.col("TX_DATETIME"))).as_("NO_DAYS")
    ).to_pandas()

    days = int(date_info['NO_DAYS'].values[0])
    start_date = str(date_info['END_DATE'].values[0].astype('datetime64[D]'))

    # Create a Snowpark dataframe with one row for each date between the min and max transaction date
    df_days = session.range(days+1).with_column(
        "TX_DATE", F.to_date(F.dateadd("DAY", F.col("id"), F.lit(start_date)))
    )
    df_days.write.save_as_table("df_days", create_temp_table=True)
    df_days = session.table("df_days")

    # Create a distinct customer list
    distinct_customers = df_trx.select("CUSTOMER_ID").distinct()
    distinct_customers.write.save_as_table("CUSTOMERS", create_temp_table=True)
    df_customers = session.table("CUSTOMERS")

    # Cross join customers with dates
    df_cust_day = df_customers.cross_join(df_days)
    df_cust_day.write.save_as_table("df_cust_day", create_temp_table=True)
    df_cust_day = session.table("df_cust_day")

    # Aggregate transactions by CUSTOMER_ID and TX_DATE (date part of TX_DATETIME)
    df_aggregated = df_trx.with_column("TX_DATE", F.to_date(F.col("TX_DATETIME"))) \
                          .group_by("CUSTOMER_ID", "TX_DATE") \
                          .agg(
                              F.sum("TX_AMOUNT").as_("TOT_AMOUNT"),
                              F.count("TX_AMOUNT").as_("NO_TRX")
                          )

    # Join the aggregated data with the customer-date combinations
    df_cust_trx_day = df_cust_day.join(
        df_aggregated,
        (df_cust_day["CUSTOMER_ID"] == df_aggregated["CUSTOMER_ID"]) &
        (df_cust_day["TX_DATE"] == df_aggregated["TX_DATE"]),
        "leftouter"
    ).select(
        df_cust_day["CUSTOMER_ID"].as_("CUSTOMER_ID"),
        df_cust_day["TX_DATE"].as_('TX_DATE'),
        F.coalesce(df_aggregated["TOT_AMOUNT"], F.lit(0)).as_("TOT_AMOUNT"),
        F.coalesce(df_aggregated["NO_TRX"], F.lit(0)).as_("NO_TRX")
    )

    # Create rolling windows for 7 and 30 days
    cust_date = Window.partition_by(F.col("CUSTOMER_ID")).orderBy(F.col("TX_DATE"))
    win_7d_cust = cust_date.rowsBetween(-7, -1)
    win_30d_cust = cust_date.rowsBetween(-30, -1)

    df_cust_feat_day = df_cust_trx_day.select(
        F.col("TX_DATE"),
        F.col("CUSTOMER_ID").as_("CUSTOMER_ID"),
        F.col("NO_TRX"),
        F.col("TOT_AMOUNT"),
        F.lag(F.col("NO_TRX"), 1).over(cust_date).as_("CUST_TX_PREV_1"),
        F.sum(F.col("NO_TRX")).over(win_7d_cust).as_("CUST_TX_PREV_7"),
        F.sum(F.col("NO_TRX")).over(win_30d_cust).as_("CUST_TX_PREV_30"),
        F.lag(F.col("TOT_AMOUNT"), 1).over(cust_date).as_("CUST_TOT_AMT_PREV_1"),
        F.sum(F.col("TOT_AMOUNT")).over(win_7d_cust).as_("CUST_TOT_AMT_PREV_7"),
        F.sum(F.col("TOT_AMOUNT")).over(win_30d_cust).as_("CUST_TOT_AMT_PREV_30")
    )

    # Join back to original transactions to get the exact TX_DATETIME
    
    df_final = df_cust_feat_day.join(
        df_trx,
        ((df_cust_feat_day["CUSTOMER_ID"] == df_trx["CUSTOMER_ID"]) &
        (df_cust_feat_day["TX_DATE"] == F.to_date(df_trx["TX_DATETIME"]))),
        "inner"
    ).select(
        df_trx["CUSTOMER_ID"].as_("CUSTOMER_ID"),
        df_trx["TX_DATETIME"],
        df_cust_feat_day["NO_TRX"],
        df_cust_feat_day["TOT_AMOUNT"],
        df_cust_feat_day["CUST_TX_PREV_1"],
        df_cust_feat_day["CUST_TX_PREV_7"],
        df_cust_feat_day["CUST_TX_PREV_30"],
        df_cust_feat_day["CUST_TOT_AMT_PREV_1"],
        df_cust_feat_day["CUST_TOT_AMT_PREV_7"],
        df_cust_feat_day["CUST_TOT_AMT_PREV_30"]
    )
    
    return df_final
