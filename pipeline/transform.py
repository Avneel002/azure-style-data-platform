import pandas as pd
from datetime import datetime
from pathlib import Path

PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def transform_csv_data(df):
    print("Transforming sales data")

    df_time = df[["date"]].drop_duplicates().copy()
    df_time["date_id"] = range(1, len(df_time) + 1)
    df_time["year"] = pd.to_datetime(df_time["date"]).dt.year
    df_time["month"] = pd.to_datetime(df_time["date"]).dt.month
    df_time["quarter"] = pd.to_datetime(df_time["date"]).dt.quarter
    df_time["day_of_week"] = pd.to_datetime(df_time["date"]).dt.dayofweek
    df_time["month_name"] = pd.to_datetime(df_time["date"]).dt.month_name()
    df_time["is_weekend"] = df_time["day_of_week"].isin([5, 6])

    df_product = df[["product_id"]].drop_duplicates().copy()
    df_product["product_key"] = range(1, len(df_product) + 1)
    df_product["product_category"] = df_product["product_id"].apply(
        lambda x: f"Category_{int(x.replace('PROD', '')) % 3 + 1}"
    )

    df_customer = df[["customer_id", "region"]].drop_duplicates().copy()
    df_customer["customer_key"] = range(1, len(df_customer) + 1)

    df_fact = df.copy()
    df_fact = df_fact.merge(df_time[["date", "date_id"]], on="date", how="left")
    df_fact = df_fact.merge(df_product[["product_id", "product_key"]], on="product_id", how="left")
    df_fact = df_fact.merge(df_customer[["customer_id", "customer_key"]], on="customer_id", how="left")

    df_fact["revenue"] = df_fact["total_amount"]
    df_fact["cost"] = df_fact["total_amount"] * 0.6
    df_fact["profit"] = df_fact["revenue"] - df_fact["cost"]
    df_fact["profit_margin"] = (df_fact["profit"] / df_fact["revenue"]) * 100

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
            "profit_margin"
        ]
    ].copy()

    return {
        "fact_sales": fact_sales,
        "dim_time": df_time,
        "dim_product": df_product,
        "dim_customer": df_customer
    }


def transform_api_data(df):
    print("Transforming user data")

    df_user = df.copy()
    df_user["user_key"] = range(1, len(df_user) + 1)
    df_user["full_name"] = df_user["name"]

    if "email" in df_user.columns:
        df_user["email_domain"] = df_user["email"].str.split("@").str[1]

    df_user["name_length"] = df_user["name"].str.len()

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
        "name_length"
    ]

    columns = [c for c in columns if c in df_user.columns]
    df_user = df_user[columns]

    return {
        "dim_user": df_user
    }


def save_transformed_data(data_dict, source_type):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    saved = []

    for table, df in data_dict.items():
        file_path = PROCESSED_DIR / f"{table}_{source_type}_{timestamp}.parquet"
        df.to_parquet(file_path, index=False, engine="pyarrow")
        saved.append(file_path.name)
        print(f"Saved {table}: {len(df)} records")

    return saved


def transform_data(df, source_type):
    print(f"Starting transformation for {source_type}")

    if source_type == "csv":
        transformed = transform_csv_data(df)
    elif source_type == "api":
        transformed = transform_api_data(df)
    else:
        raise ValueError(f"Unknown source type: {source_type}")

    save_transformed_data(transformed, source_type)
    print(f"Transformation finished for {source_type}")

    return transformed


def generate_summary_stats(transformed_data, source_type):
    if source_type == "csv":
        fact = transformed_data["fact_sales"]
        return {
            "total_transactions": len(fact),
            "total_revenue": round(fact["revenue"].sum(), 2),
            "total_profit": round(fact["profit"].sum(), 2),
            "avg_transaction_value": round(fact["revenue"].mean(), 2),
            "total_quantity_sold": int(fact["quantity"].sum()),
            "avg_profit_margin": round(fact["profit_margin"].mean(), 2)
        }

    if source_type == "api":
        dim = transformed_data["dim_user"]
        return {
            "total_users": len(dim),
            "unique_domains": dim["email_domain"].nunique() if "email_domain" in dim.columns else 0,
            "unique_cities": dim["city"].nunique() if "city" in dim.columns else 0
        }

    return {}


if __name__ == "__main__":
    print("TESTING DATA TRANSFORMATION MODULE")

    sample_sales = pd.DataFrame({
        "transaction_id": ["TXN001", "TXN002", "TXN003", "TXN004", "TXN005"],
        "date": pd.to_datetime([
            "2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"
        ]),
        "product_id": ["PROD001", "PROD002", "PROD001", "PROD003", "PROD002"],
        "customer_id": ["CUST001", "CUST002", "CUST001", "CUST003", "CUST002"],
        "quantity": [5, 10, 3, 7, 2],
        "unit_price": [25.50, 30.00, 25.50, 15.00, 30.00],
        "total_amount": [127.50, 300.00, 76.50, 105.00, 60.00],
        "region": ["North", "South", "North", "East", "South"]
    })

    result_csv = transform_data(sample_sales, "csv")
    print(generate_summary_stats(result_csv, "csv"))

    sample_users = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["John Doe", "Jane Smith", "Bob Johnson"],
        "username": ["johnd", "janes", "bobj"],
        "email": ["john@example.com", "jane@company.org", "bob@test.net"],
        "phone": ["555-1234", "555-5678", "555-9012"],
        "website": ["john.com", "jane.com", "bob.com"],
        "company_name": ["Acme Corp", "Tech Inc", "Data Ltd"],
        "city": ["Adelaide", "Sydney", "Melbourne"],
        "street": ["123 Main St", "456 Oak Ave", "789 Pine Rd"],
        "zipcode": ["5000", "2000", "3000"]
    })

    result_api = transform_data(sample_users, "api")
    print(generate_summary_stats(result_api, "api"))

    print("TRANSFORMATION MODULE TEST COMPLETE")
