import os
import sys

# добавить корень проекта в PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task
from dask_jobs.customer_analysis import load_raw_data, compute_customer_metrics
from dask_jobs.db_utils import save_customer_metrics

DATA_PATH = "data/online_retail.csv"


@task
def extract(path: str):
    return path


@task
def transform(path: str):
    ddf = load_raw_data(path)
    metrics_df = compute_customer_metrics(ddf)  # pandas DataFrame
    return metrics_df


@task
def load(metrics_df):
    print("LOAD TASK STARTED")
    print(metrics_df.head())
    save_customer_metrics(metrics_df)
    print("Saved to PostgreSQL")


@flow(log_prints=True)
def etl_flow():
    print("FLOW STARTED")
    path = extract(DATA_PATH)
    metrics = transform(path)
    load(metrics)


if __name__ == "__main__":
    etl_flow()

