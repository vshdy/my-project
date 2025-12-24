import dask.dataframe as dd
import pandas as pd

def load_raw_data(path: str):
    # Читаем данные через Dask
    ddf = dd.read_csv(
        path,
        encoding='ISO-8859-1',
        dtype={
            "InvoiceNo": "object",
            "StockCode": "object",
            "CustomerID": "float64",
        },
        parse_dates=['InvoiceDate'], 
        assume_missing=True,
    )
    return ddf

def compute_customer_metrics(ddf):
    ddf = ddf.dropna(subset=['CustomerID'])
    ddf['LineTotal'] = ddf['Quantity'] * ddf['UnitPrice']
    ddf = ddf[ddf['LineTotal'] > 0]

    max_date = ddf['InvoiceDate'].max().compute()
    
    snapshot_date = max_date + pd.Timedelta(days=1)

    rfm = ddf.groupby('CustomerID').agg({
        'InvoiceDate': 'max',
        'InvoiceNo': 'count',
        'LineTotal': 'sum',
        'UnitPrice': 'mean',
        'Country': 'first' 
    })

    rfm_pd = rfm.compute()

    rfm_pd['recency'] = (snapshot_date - rfm_pd['InvoiceDate']).dt.days
    rfm_pd = rfm_pd.rename(columns={
        'InvoiceNo': 'frequency',
        'LineTotal': 'monetary',
        'UnitPrice': 'avg_unit_price' 
    })
    
    return rfm_pd[['recency', 'frequency', 'monetary', 'avg_unit_price', 'Country']].reset_index()
