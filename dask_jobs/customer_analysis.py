import dask.dataframe as dd

def load_raw_data(path: str):
    ddf = dd.read_csv(
        path,
        dtype={
            "InvoiceNo": "object",
            "StockCode": "object",
            "Description": "object",
            "Quantity": "float64",
            "InvoiceDate": "object",
            "UnitPrice": "float64",
            "CustomerID": "float64",
            "Country": "object",
        },
        assume_missing=True,
    )
    return ddf

def compute_customer_metrics(ddf):
    ddf["LineTotal"] = ddf["Quantity"] * ddf["UnitPrice"]

    grouped = ddf.groupby("CustomerID")["LineTotal"].agg(
        ["count", "sum", "mean"]
    )

    result = grouped.reset_index().rename(
        columns={
            "count": "order_lines_count",
            "sum": "revenue",
            "mean": "avg_line_revenue",
        }
    )
    return result.compute() 
