import os
import sys
from prefect import flow, task

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dask_jobs.customer_analysis import load_raw_data, compute_customer_metrics
from dask_jobs.db_utils import save_customer_metrics

DATA_PATH = "data/online_retail.csv"

@task(name="Extract CSV")
def extract(path: str):
    return path

@task(name="Dask Transformation (RFM)")
def transform(path: str):
    ddf = load_raw_data(path)
    metrics_df = compute_customer_metrics(ddf)
    return metrics_df

@task(name="Load to PostgreSQL")
def load(metrics_df):
    save_customer_metrics(metrics_df, table_name="customer_rfm")
    print("Данные успешно сохранены в таблицу customer_rfm")

@flow(name="Main ETL Pipeline", log_prints=True)
def etl_flow():
    path = extract(DATA_PATH)
    metrics = transform(path)
    load(metrics)

if __name__ == "__main__":
    etl_flow()