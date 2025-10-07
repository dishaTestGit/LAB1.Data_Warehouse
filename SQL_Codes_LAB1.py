from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

# ----------------------------
# Build Connection
# ----------------------------

def return_snowflake_conn():

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()
# ----------------------------
# Extract Task
# ----------------------------
@task
def extract_stock_data(symbol):
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="90d")
        df.reset_index(inplace=True)
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
        return df.to_json(orient="records")
    except Exception as e:
        raise e

# ----------------------------
# Transform Task 
# ----------------------------
@task
def transform_stock_data(json_df, symbol):
    try:
        df = pd.read_json(json_df, convert_dates=False)
        df['Date'] = df['Date'].astype(str)
        df = df.rename(
            columns={
                "Date": "trade_date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "trade_volume",
            }
        )
        df["symbol"] = symbol

        print(f"Transformed {len(df)} rows for {symbol}")
        return df.to_json(orient="records")
    except Exception as e:
        raise e


# --------------------------------
# Load Task using SQL transaction
# --------------------------------
@task
def load(records_json, symbol):
    target_table = "USER_DB_OTTER.RAW.market_data_LAB1"
    conn = return_snowflake_conn()
    try:
        conn.execute("BEGIN;")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                trade_date DATE,
                open NUMBER,
                high NUMBER,
                low NUMBER,
                close NUMBER,
                trade_volume NUMBER,
                symbol VARCHAR,
                PRIMARY KEY (trade_date, symbol)
            );
        """)
        conn.execute(f"DELETE FROM {target_table}")
        
        import json
        from pandas import json_normalize
        data_list = json.loads(records_json)
        df = json_normalize(data_list)

        df["trade_date"] = pd.to_datetime(df["trade_date"])
        insert_sql = f"""
            INSERT INTO {target_table} 
            (trade_date, open, high, low, close, trade_volume, symbol)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        data = [
            (
                row["trade_date"].strftime("%Y-%m-%d"), 
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                int(row["trade_volume"]),
                symbol
            )
            for _, row in df.iterrows()
        ]

        conn.executemany(insert_sql, data)
        conn.execute("COMMIT")

    except Exception as e:
        conn.execute("ROLLBACK")
        raise e


        
# ----------------------------
# Airflow DAG Definition
# ----------------------------


with DAG(
    dag_id="yfinance_sql_code",
    schedule="30 2 * * *",  
    catchup=False,
    tags=["ETL"],
) as dag:

    symbol = "ORCL"
    extracted = extract_stock_data(symbol)
    transformed = transform_stock_data(extracted, symbol)
    load_task = load(transformed, symbol=symbol)

    extracted >> transformed >> load_task
