from sqlalchemy import create_engine

def get_engine():
    # user = "retail_user"
    # password = "retail_password"
    # host = "localhost"
    # port = 5432
    # db = "retail_db"

    # используем pg8000 вместо psycopg2
    url = f"postgresql+pg8000://retail_user:retail_password@localhost:5433/retail_db"
    print("DB URL:", url)
    engine = create_engine(url)
    return engine

def save_customer_metrics(df, table_name: str = "customer_metrics"):
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists="replace", index=False)
