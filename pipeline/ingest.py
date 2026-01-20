import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
import json

RAW_DATA_DIR = Path("data/raw")
METADATA_DIR = Path("metadata")
LOGS_DIR = Path("logs")

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
METADATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)


def log_ingestion(source_type, status, record_count, error=None):
    timestamp = datetime.now().isoformat()
    log_entry = {
        "timestamp": timestamp,
        "source_type": source_type,
        "status": status,
        "record_count": record_count,
        "error": error
    }
    log_file = LOGS_DIR / "ingestion.log"
    with open(log_file, "a") as f:
        f.write(json.dumps(log_entry) + "\n")
    return log_entry


def ingest_csv():
    print("Ingesting CSV data source (Sales Transactions)...")
    try:
        sample_data = {
            'transaction_id': [f'TXN{str(i).zfill(6)}' for i in range(1, 101)],
            'date': pd.date_range('2025-01-01', periods=100, freq='D').strftime('%Y-%m-%d').tolist(),
            'product_id': [f'PROD{(i % 10) + 1:03d}' for i in range(100)],
            'customer_id': [f'CUST{(i % 25) + 1:04d}' for i in range(100)],
            'quantity': [((i % 5) + 1) for i in range(100)],
            'unit_price': [round(10 + (i % 50) * 2.5, 2) for i in range(100)],
            'region': [['North', 'South', 'East', 'West'][i % 4] for i in range(100)]
        }
        df = pd.DataFrame(sample_data)
        df['total_amount'] = df['quantity'] * df['unit_price']
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = RAW_DATA_DIR / f"sales_raw_{timestamp}.csv"
        df.to_csv(raw_file, index=False)
        metadata = {
            "source": "CSV",
            "source_type": "Sales Transactions",
            "filename": raw_file.name,
            "ingestion_time": timestamp,
            "record_count": len(df),
            "columns": list(df.columns),
            "file_size_bytes": raw_file.stat().st_size
        }
        metadata_file = METADATA_DIR / f"csv_metadata_{timestamp}.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)
        log_ingestion("CSV", "SUCCESS", len(df))
        print(f"Ingested {len(df)} records from CSV")
        print(f"Saved to: {raw_file}")
        print(f"Metadata: {metadata_file}")
        return df
    except Exception as e:
        log_ingestion("CSV", "FAILED", 0, str(e))
        raise Exception(f"CSV ingestion failed: {str(e)}")


def ingest_api():
    print("Ingesting API data source (User Profiles)...")
    try:
        api_url = "https://jsonplaceholder.typicode.com/users"
        print(f"Calling API: {api_url}")
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        api_data = response.json()
        df = pd.DataFrame(api_data)
        if 'address' in df.columns:
            df['city'] = df['address'].apply(lambda x: x.get('city', '') if isinstance(x, dict) else '')
            df['street'] = df['address'].apply(lambda x: x.get('street', '') if isinstance(x, dict) else '')
            df['zipcode'] = df['address'].apply(lambda x: x.get('zipcode', '') if isinstance(x, dict) else '')
            df = df.drop('address', axis=1)
        if 'company' in df.columns:
            df['company_name'] = df['company'].apply(lambda x: x.get('name', '') if isinstance(x, dict) else '')
            df = df.drop('company', axis=1)
        if 'geo' in df.columns:
            df = df.drop('geo', axis=1)
        df['ingestion_timestamp'] = datetime.now()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = RAW_DATA_DIR / f"api_users_raw_{timestamp}.csv"
        df.to_csv(raw_file, index=False)
        metadata = {
            "source": "API",
            "source_type": "User Profiles",
            "endpoint": api_url,
            "ingestion_time": timestamp,
            "record_count": len(df),
            "columns": list(df.columns),
            "status_code": response.status_code
        }
        metadata_file = METADATA_DIR / f"api_metadata_{timestamp}.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)
        log_ingestion("API", "SUCCESS", len(df))
        print(f"Ingested {len(df)} records from API")
        print(f"Endpoint: {api_url}")
        print(f"Saved to: {raw_file}")
        print(f"Metadata: {metadata_file}")
        return df
    except requests.exceptions.RequestException as e:
        log_ingestion("API", "FAILED", 0, str(e))
        raise Exception(f"API ingestion failed: {str(e)}")
    except Exception as e:
        log_ingestion("API", "FAILED", 0, str(e))
        raise Exception(f"API processing failed: {str(e)}")


if __name__ == "__main__":
    print("="*60)
    print("TESTING DATA INGESTION MODULE")
    print("="*60)
    print("\n[1] CSV Data Ingestion")
    print("-"*60)
    csv_df = ingest_csv()
    print("\nSample data preview:")
    print(csv_df.head(3))
    print("\n" + "="*60)
    print("\n[2] API Data Ingestion")
    print("-"*60)
    api_df = ingest_api()
    print("\nSample data preview:")
    print(api_df.head(3))
    print("\n" + "="*60)
    print("INGESTION MODULE TEST COMPLETE")
    print("="*60)
