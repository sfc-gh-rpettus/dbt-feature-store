import snowflake.snowpark.functions as F
from snowflake.snowpark import Window

def model(dbt, session):
    # Reference the stg_orders table
    df_orders = dbt.ref("stg_orders")

    # Convert order date to a date format if needed
    df_orders = df_orders.with_column("O_ORDERDATE", F.to_date(F.col("O_ORDERDATE")))

    # Generate a distinct list of customer keys and the full range of order dates
    distinct_customers = df_orders.select("O_CUSTKEY").distinct()
    
    # Get the range of dates (min and max order date) as columns
    date_range = df_orders.select(
        F.min("O_ORDERDATE").as_("START_DATE"),
        F.max("O_ORDERDATE").as_("END_DATE")
    ).first()

    start_date = date_range["START_DATE"]
    end_date = date_range["END_DATE"]

    # Calculate the number of days between start and end date
    num_days = (end_date - start_date).days

    # Create a dataframe with all dates between the start and end date
    date_series = session.range(num_days + 1)\
                         .with_column("ORDER_DATE", F.dateadd("day", F.col("id"), F.lit(start_date)))

    # Create a cartesian product of customers and dates to ensure every customer has a row for every date
    customer_dates = distinct_customers.cross_join(date_series)

    # Join the customer_dates with the actual orders to fill in missing dates with zero orders and amounts
    df_cust_full = customer_dates.join(
        df_orders,
        (customer_dates["O_CUSTKEY"] == df_orders["O_CUSTKEY"]) &
        (customer_dates["ORDER_DATE"] == df_orders["O_ORDERDATE"]),
        "left"
    ).select(
        customer_dates["O_CUSTKEY"].as_("CUSTOMER_ID"),
        customer_dates["ORDER_DATE"],
        F.coalesce(F.col("O_TOTALPRICE"), F.lit(0)).as_("TOT_AMOUNT"),
        F.when(F.col("O_ORDERKEY").isNotNull(), F.lit(1)).otherwise(F.lit(0)).as_("NO_ORDERS"),
        F.when(F.col("O_ORDERPRIORITY") == "1-URGENT", F.col("O_TOTALPRICE")).otherwise(F.lit(0)).as_("URGENT_TOT_AMOUNT"),
        F.when(F.col("O_ORDERPRIORITY") == "1-URGENT", F.lit(1)).otherwise(F.lit(0)).as_("URGENT_NO_ORDERS")
    )

    # Create the window/partition-by reference and window ranges for lagged features
    cust_date = Window.partition_by(F.col("CUSTOMER_ID")).orderBy(F.col("ORDER_DATE"))
    win_30d_cust = cust_date.rangeBetween(-30, -1)
    win_60d_cust = cust_date.rangeBetween(-60, -1)
    win_90d_cust = cust_date.rangeBetween(-90, -1)

    # Create the features based on previous periods
    df_cust_feat_day = df_cust_full.select(
        F.col("CUSTOMER_ID"),
        F.col("ORDER_DATE"),
        F.sum("NO_ORDERS").over(win_30d_cust).as_("CUST_ORDER_CNT_30"),
        F.sum("NO_ORDERS").over(win_60d_cust).as_("CUST_ORDER_CNT_60"),
        F.sum("NO_ORDERS").over(win_90d_cust).as_("CUST_ORDER_CNT_90"),
        F.sum("TOT_AMOUNT").over(win_30d_cust).as_("CUST_ORDER_SUM_30"),
        F.sum("TOT_AMOUNT").over(win_60d_cust).as_("CUST_ORDER_SUM_60"),
        F.sum("TOT_AMOUNT").over(win_90d_cust).as_("CUST_ORDER_SUM_90"),
        F.sum("URGENT_NO_ORDERS").over(win_30d_cust).as_("CUST_URGENT_ORDER_CNT_30"),
        F.sum("URGENT_NO_ORDERS").over(win_60d_cust).as_("CUST_URGENT_ORDER_CNT_60"),
        F.sum("URGENT_NO_ORDERS").over(win_90d_cust).as_("CUST_URGENT_ORDER_CNT_90"),
        F.sum("URGENT_TOT_AMOUNT").over(win_30d_cust).as_("CUST_URGENT_ORDER_SUM_30"),
        F.sum("URGENT_TOT_AMOUNT").over(win_60d_cust).as_("CUST_URGENT_ORDER_SUM_60"),
        F.sum("URGENT_TOT_AMOUNT").over(win_90d_cust).as_("CUST_URGENT_ORDER_SUM_90")
    )

    # Filter the results to include only rows with original order dates
    df_final = df_cust_feat_day.join(
        df_orders.select(F.col("O_CUSTKEY").as_("CUSTOMER_ID"), F.col("O_ORDERDATE").as_("ORDER_DATE")),
        ["CUSTOMER_ID", "ORDER_DATE"]
    )

    # Select the final features with customer_id and order_date
    df_final = df_final.select(
        F.col("CUSTOMER_ID"),
        F.col("ORDER_DATE"),
        F.col("CUST_ORDER_CNT_30"),
        F.col("CUST_ORDER_CNT_60"),
        F.col("CUST_ORDER_CNT_90"),
        F.col("CUST_ORDER_SUM_30"),
        F.col("CUST_ORDER_SUM_60"),
        F.col("CUST_ORDER_SUM_90"),
        F.col("CUST_URGENT_ORDER_CNT_30"),
        F.col("CUST_URGENT_ORDER_CNT_60"),
        F.col("CUST_URGENT_ORDER_CNT_90"),
        F.col("CUST_URGENT_ORDER_SUM_30"),
        F.col("CUST_URGENT_ORDER_SUM_60"),
        F.col("CUST_URGENT_ORDER_SUM_90")
    )

    return df_final
