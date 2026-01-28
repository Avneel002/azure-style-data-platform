import pandas as pd
from datetime import datetime
from pathlib import Path

PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def transform_csv_data(df):
    df_time = df[["date"]].drop_duplicates().copy()
    df_time["date_id"] = range(1, len(df_time) + 1)

    dates = pd.to_datetime(df_time["date"])
    df_time["year"] = dates.dt.year
    df_time["month"] = dates.dt.month
    df_time["quarter"] = dates.dt.quarter
    df_time["day_of_week"] = dates.dt.dayofweek
    df_time["month_name"] = dates.dt.month_name()
    df_time["is_weekend"] = df_time["day_of_week"].isin([5, 6])

    df_product = df[["product_id"]].drop_duplicates().copy()
    df_product["product_key"] = range(1, len(df_product) + 1)
    df_product["product_category"] = df_product["product_id"].apply(
        lambda x: f"Category_{int(x.replace('PROD', '')) % 3 + 1}"
    )
    total_customers = df["customer_id"].nunique()
    df_customer = (
        df.sort_values("date")
          .groupby("customer_id", as_index=False)
          .first()[["customer_id", "region"]]
    )
    unique_customers = len(df_customer)
    duplicates_removed = total_customers - unique_customers
    if duplicates_removed > 0:
        print(f"⚠ {duplicates_removed} duplicate customer(s) removed to ensure UNIQUE customer_id")
    df_customer["customer_key"] = range(1, len(df_customer) + 1)
    df_fact = (
        df.merge(df_time[["date", "date_id"]], on="date", how="left")
          .merge(df_product[["product_id", "product_key"]], on="product_id", how="left")
          .merge(df_customer[["customer_id", "customer_key"]], on="customer_id", how="left")
    )

    df_fact["revenue"] = df_fact["total_amount"]
    df_fact["cost"] = df_fact["total_amount"] * 0.6
    df_fact["profit"] = df_fact["revenue"] - df_fact["cost"]
    df_fact["profit_margin"] = (
        df_fact["profit"] / df_fact["revenue"].replace(0, pd.NA)
    ) * 100

    fact_sales = df_fact[
        [
            "transaction_id",
            "date_id",
            "product_key",
            "customer_key",
            "quantity",
            "unit_price",
            "revenue",
            "cost",
            "profit",
            "profit_margin",
        ]
    ].copy()

    return {
        "fact_sales": fact_sales,
        "dim_time": df_time,
        "dim_product": df_product,
        "dim_customer": df_customer,
    }


def transform_api_data(df):
    df_user = df.copy()
    df_user["user_key"] = range(1, len(df_user) + 1)
    df_user["full_name"] = df_user["name"]
    df_user["name_length"] = df_user["name"].str.len()

    if "email" in df_user.columns:
        df_user["email_domain"] = df_user["email"].str.split("@").str[1]

    columns = [
        "user_key",
        "id",
        "full_name",
        "username",
        "email",
        "email_domain",
        "phone",
        "website",
        "company_name",
        "city",
        "street",
        "zipcode",
        "name_length",
    ]

    df_user = df_user[[c for c in columns if c in df_user.columns]]

    return {"dim_user": df_user}


def save_transformed_data(data, source_type):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for table, df in data.items():
        path = PROCESSED_DIR / f"{table}_{source_type}_{timestamp}.parquet"
        df.to_parquet(path, index=False, engine="pyarrow")


def transform_data(df, source_type):
    if source_type == "csv":
        transformed = transform_csv_data(df)
        dim_customer = transformed.get("dim_customer")
        if dim_customer is not None:
            total = df["customer_id"].nunique()
            unique = len(dim_customer)
            removed = total - unique
            if removed > 0:
                print(f"INFO: Removed {removed} duplicate customer(s) before database load")
    elif source_type == "api":
        transformed = transform_api_data(df)
    else:
        raise ValueError("Unsupported source type")

    save_transformed_data(transformed, source_type)
    return transformed


def generate_summary_stats(data, source_type):
    if source_type == "csv":
        fact = data["fact_sales"]
        return {
            "total_transactions": len(fact),
            "total_revenue": round(fact["revenue"].sum(), 2),
            "total_profit": round(fact["profit"].sum(), 2),
            "avg_transaction_value": round(fact["revenue"].mean(), 2),
            "total_quantity_sold": int(fact["quantity"].sum()),
            "avg_profit_margin": round(fact["profit_margin"].mean(), 2),
        }

    if source_type == "api":
        dim = data["dim_user"]
        return {
            "total_users": len(dim),
            "unique_domains": dim["email_domain"].nunique() if "email_domain" in dim else 0,
            "unique_cities": dim["city"].nunique() if "city" in dim else 0,
        }

    return {}
