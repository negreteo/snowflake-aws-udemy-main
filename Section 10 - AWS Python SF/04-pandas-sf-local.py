import pandas as pd
import sys
import snowflake.connector
# from awsglue.utils import getResolvedOptions

con = snowflake.connector.connect(
    user="negreteo",
    password="Udemy@123",
    account="qdqhjib-njb76303",
    warehouse="compute_wh",
    database="ecommerce_db",
    schema="ECOMMERCE_DEV",
    role='SYSADMIN',
    session_parameters={
        'TIMEZONE': 'UTC',
    }
)

try:
    sql = """
        select * from lineitem limit 10
    """
    data_agg = pd.read_sql(sql, con)
    print(data_agg.head())
finally:
    con.close()