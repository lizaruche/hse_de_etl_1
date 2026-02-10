from __future__ import annotations

import os
from datetime import timedelta

import pendulum
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def create_temperature_table():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    with conn.cursor() as cursor:
        cursor.execute("""
            create table if not exists temperature_readings (
                id serial primary key,
                noted_date date not null,
                temp numeric not null,
                out_in varchar(10) not null,
                device_id varchar(255)
            );
            
            create table if not exists temperature_metrics (
                id serial primary key,
                metric_type varchar(50) not null,
                noted_date date not null,
                avg_temp numeric not null,
                min_temp numeric,
                max_temp numeric,
                created_at timestamp default current_timestamp
            );
        """)
        conn.commit()
    conn.close()

def load_local_dataset():
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    csv_path = os.path.join(project_root, "airflow", "datasets", "IOT-temp.csv")
    return pd.read_csv(csv_path)


def transform_temperature_data(df: pd.DataFrame):
    column_mapping = {}
    for col in df.columns:
        col_lower = col.lower()
        if "date" in col_lower or "noted" in col_lower:
            column_mapping[col] = "noted_date"
        elif "temp" in col_lower:
            column_mapping[col] = "temp"
        elif col_lower in ["out/in", "out_in", "in/out", "location"]:
            column_mapping[col] = "out_in"
        elif "device" in col_lower or "id" in col_lower:
            column_mapping[col] = "device_id"

    df = df.rename(columns=column_mapping)
    print(f"New column names: {df.columns.tolist()}")

    # filter out/in = in
    df["out_in"] = df["out_in"].astype(str).str.strip().str.lower()
    df = df[df["out_in"] == "in"].copy()

    # transform noted_date to date type
    date_formats = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
    ]
    df["noted_date"] = pd.to_datetime(
        df["noted_date"], infer_datetime_format=True, errors="coerce"
    )

    if df["noted_date"].isna().any():
        for fmt in date_formats:
            mask = df["noted_date"].isna()
            if not mask.any():
                break
            parsed = pd.to_datetime(
                df.loc[mask, "noted_date"], format=fmt, errors="coerce"
            )
            df.loc[mask, "noted_date"] = parsed

    df["noted_date"] = df["noted_date"].dt.date
    df = df.dropna(subset=["noted_date"])

    df["temp"] = pd.to_numeric(df["temp"], errors="coerce")
    df = df.dropna(subset=["temp"])

    temp_5th = df["temp"].quantile(0.05)
    temp_95th = df["temp"].quantile(0.95)

    df = df[(df["temp"] >= temp_5th) & (df["temp"] <= temp_95th)].copy()

    daily_stats = (
        df.groupby("noted_date")
        .agg({"temp": ["mean", "min", "max"]})
        .reset_index()
    )
    daily_stats.columns = ["noted_date", "avg_temp", "min_temp", "max_temp"]

    hottest_days = daily_stats.nlargest(5, "avg_temp")
    coldest_days = daily_stats.nsmallest(5, "avg_temp")

    print("\n=== 5 Hottest Days ===")
    print(hottest_days.to_string())
    print("\n=== 5 Coldest Days ===")
    print(coldest_days.to_string())

    return df, hottest_days, coldest_days


def load_to_db(
    df: pd.DataFrame, hottest_days: pd.DataFrame, coldest_days: pd.DataFrame
):
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow",
    )

    try:
        with conn.cursor() as cursor:
            insert_data = []
            for _, row in df.iterrows():
                insert_data.append(
                    (
                        row["noted_date"],
                        float(row["temp"]),
                        str(row["out_in"]),
                        str(row.get("device_id", "")) if "device_id" in row else None,
                    )
                )

            if insert_data:
                insert_query = """
                    INSERT INTO temperature_readings (noted_date, temp, out_in, device_id)
                    VALUES %s"""
                execute_values(cursor, insert_query, insert_data)

            hottest_data = []
            for _, row in hottest_days.iterrows():
                hottest_data.append(
                    (
                        "hottest",
                        row["noted_date"],
                        float(row["avg_temp"]),
                        float(row["min_temp"]),
                        float(row["max_temp"]),
                    )
                )

            if hottest_data:
                metrics_query = """
                    INSERT INTO temperature_metrics (metric_type, noted_date, avg_temp, min_temp, max_temp)
                    VALUES %s"""
                execute_values(cursor, metrics_query, hottest_data)

            coldest_data = []
            for _, row in coldest_days.iterrows():
                coldest_data.append(
                    (
                        "coldest",
                        row["noted_date"],
                        float(row["avg_temp"]),
                        float(row["min_temp"]),
                        float(row["max_temp"]),
                    )
                )

            if coldest_data:
                execute_values(cursor, metrics_query, coldest_data)

        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {str(e)}")
        raise e
    finally:
        conn.close()


def load_historical_temperature_data():
    df_raw = load_local_dataset()
    df, hottest_days, coldest_days = transform_temperature_data(df_raw)

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow",
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE temperature_readings, temperature_metrics;")
        conn.commit()
    finally:
        conn.close()

    load_to_db(df, hottest_days, coldest_days)


def load_incremental_temperature_data(days_back: int = 3):
    df_raw = load_local_dataset()
    df_all, hottest_all, coldest_all = transform_temperature_data(df_raw)

    today = pendulum.now("UTC").date()
    cutoff = today - timedelta(days=days_back)

    df_inc = df_all[df_all["noted_date"] >= cutoff].copy()
    hottest_inc = hottest_all[hottest_all["noted_date"] >= cutoff].copy()
    coldest_inc = coldest_all[coldest_all["noted_date"] >= cutoff].copy()

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow",
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM temperature_readings WHERE noted_date >= %s;", (cutoff,)
            )
            cursor.execute(
                "DELETE FROM temperature_metrics WHERE noted_date >= %s;", (cutoff,)
            )
        conn.commit()
    finally:
        conn.close()

    load_to_db(df_inc, hottest_inc, coldest_inc)

with DAG(
    dag_id="temperature_etl_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "temperature", "iot"],
) as dag:
    create_table_task = PythonOperator(
        task_id="create_temperature_table",
        python_callable=create_temperature_table,
    )

    full_load_task = PythonOperator(
        task_id="load_historical_temperature_data",
        python_callable=load_historical_temperature_data,
    )

    incremental_load_task = PythonOperator(
        task_id="load_incremental_temperature_data",
        python_callable=load_incremental_temperature_data,
    )

    create_table_task >> [full_load_task, incremental_load_task]
