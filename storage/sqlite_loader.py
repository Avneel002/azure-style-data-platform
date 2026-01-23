import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

DB_DIR = Path("storage")
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / "contoso_analytics.db"


class ContosoDatabase:

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()

    def create_schema(self):
        print("Creating database schema")

        cursor = self.conn.cursor()

        tables = [
            "fact_sales",
            "dim_time",
            "dim_product",
            "dim_customer",
            "dim_user",
            "pipeline_metadata"
        ]

        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")

        cursor.execute("""
            CREATE TABLE dim_time (
                date_id INTEGER PRIMARY KEY,
                date TEXT NOT NULL UNIQUE,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                day_of_week INTEGER NOT NULL,
                month_name TEXT NOT NULL,
                is_weekend INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE dim_product (
                product_key INTEGER PRIMARY KEY,
                product_id TEXT NOT NULL UNIQUE,
                product_category TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE dim_customer (
                customer_key INTEGER PRIMARY KEY,
                customer_id TEXT NOT NULL UNIQUE,
                region TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE fact_sales (
                transaction_id TEXT PRIMARY KEY,
                date_id INTEGER NOT NULL,
                product_key INTEGER NOT NULL,
                customer_key INTEGER NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                unit_price REAL NOT NULL CHECK (unit_price > 0),
                revenue REAL NOT NULL,
                cost REAL NOT NULL,
                profit REAL NOT NULL,
                profit_margin REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (date_id) REFERENCES dim_time(date_id),
                FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
                FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key)
            )
        """)

        cursor.execute("""
            CREATE TABLE dim_user (
                user_key INTEGER PRIMARY KEY,
                id INTEGER NOT NULL UNIQUE,
                full_name TEXT NOT NULL,
                username TEXT NOT NULL,
                email TEXT NOT NULL,
                email_domain TEXT,
                phone TEXT,
                website TEXT,
                company_name TEXT,
                city TEXT,
                street TEXT,
                zipcode TEXT,
                name_length INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE pipeline_metadata (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_timestamp TIMESTAMP NOT NULL,
                pipeline_stage TEXT NOT NULL,
                source_type TEXT NOT NULL,
                records_processed INTEGER NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT,
                execution_time_seconds REAL
            )
        """)

        self.conn.commit()
        self._create_indexes()
        print("Schema ready")

    def _create_indexes(self):
        cursor = self.conn.cursor()

        indexes = [
            "CREATE INDEX idx_sales_date ON fact_sales(date_id)",
            "CREATE INDEX idx_sales_product ON fact_sales(product_key)",
            "CREATE INDEX idx_sales_customer ON fact_sales(customer_key)",
            "CREATE INDEX idx_customer_region ON dim_customer(region)",
            "CREATE INDEX idx_user_email ON dim_user(email)",
            "CREATE INDEX idx_user_city ON dim_user(city)",
            "CREATE INDEX idx_time_year_month ON dim_time(year, month)"
        ]

        for sql in indexes:
            try:
                cursor.execute(sql)
            except sqlite3.OperationalError:
                pass

        self.conn.commit()

    def load_dimension(self, df, table):
        df.to_sql(table, self.conn, if_exists="append", index=False)
        return len(df)

    def load_fact(self, df, table):
        df.to_sql(table, self.conn, if_exists="append", index=False)
        return len(df)

    def log_pipeline_run(self, stage, source, records, status, error=None, exec_time=None):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO pipeline_metadata
            (run_timestamp, pipeline_stage, source_type, records_processed, status, error_message, execution_time_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (datetime.now(), stage, source, records, status, error, exec_time))
        self.conn.commit()

    def get_summary_stats(self):
        cursor = self.conn.cursor()
        stats = {}

        cursor.execute("""
            SELECT
                COUNT(*),
                SUM(revenue),
                SUM(profit),
                AVG(revenue),
                SUM(quantity),
                AVG(profit_margin)
            FROM fact_sales
        """)

        row = cursor.fetchone()

        if row and row[0] > 0:
            stats["sales"] = {
                "total_transactions": row[0],
                "total_revenue": round(row[1], 2),
                "total_profit": round(row[2], 2),
                "avg_transaction_value": round(row[3], 2),
                "total_quantity": row[4],
                "avg_profit_margin": round(row[5], 2)
            }

        cursor.execute("SELECT COUNT(*) FROM dim_product")
        stats["total_products"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM dim_customer")
        stats["total_customers"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM dim_user")
        stats["total_users"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM dim_time")
        stats["total_time_records"] = cursor.fetchone()[0]

        return stats

    def get_sales_by_region(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                c.region,
                COUNT(*),
                SUM(s.revenue),
                SUM(s.profit),
                AVG(s.revenue)
            FROM fact_sales s
            JOIN dim_customer c ON s.customer_key = c.customer_key
            GROUP BY c.region
            ORDER BY SUM(s.revenue) DESC
        """)
        return cursor.fetchall()


def load_to_database(csv_data, api_data):
    print("Loading data into database")

    db = ContosoDatabase()

    try:
        db.connect()
        db.create_schema()

        start = datetime.now()
        db.load_dimension(csv_data["dim_time"], "dim_time")
        db.load_dimension(csv_data["dim_product"], "dim_product")
        db.load_dimension(csv_data["dim_customer"], "dim_customer")
        fact_count = db.load_fact(csv_data["fact_sales"], "fact_sales")
        db.log_pipeline_run("LOAD", "CSV", fact_count, "SUCCESS",
                            exec_time=(datetime.now() - start).total_seconds())

        start = datetime.now()
        user_count = db.load_dimension(api_data["dim_user"], "dim_user")
        db.log_pipeline_run("LOAD", "API", user_count, "SUCCESS",
                            exec_time=(datetime.now() - start).total_seconds())

        stats = db.get_summary_stats()
        regions = db.get_sales_by_region()

        print("Summary")
        print(stats)

        print("Sales by region")
        for r in regions:
            print(r)

        print("Database path:", db.db_path)
        return stats

    except Exception as e:
        db.log_pipeline_run("LOAD", "ERROR", 0, "FAILED", str(e))
        raise

    finally:
        db.close()


if __name__ == "__main__":
    print("Running database module test")

    test_dim_time = pd.DataFrame({
        "date_id": [1, 2, 3],
        "date": ["2025-01-01", "2025-01-02", "2025-01-03"],
        "year": [2025, 2025, 2025],
        "month": [1, 1, 1],
        "quarter": [1, 1, 1],
        "day_of_week": [2, 3, 4],
        "month_name": ["January", "January", "January"],
        "is_weekend": [0, 0, 0]
    })

    test_dim_product = pd.DataFrame({
        "product_key": [1, 2],
        "product_id": ["PROD001", "PROD002"],
        "product_category": ["Category_1", "Category_2"]
    })

    test_dim_customer = pd.DataFrame({
        "customer_key": [1, 2],
        "customer_id": ["CUST001", "CUST002"],
        "region": ["North", "South"]
    })

    test_fact_sales = pd.DataFrame({
        "transaction_id": ["TXN001", "TXN002", "TXN003"],
        "date_id": [1, 2, 3],
        "product_key": [1, 2, 1],
        "customer_key": [1, 2, 1],
        "quantity": [5, 10, 3],
        "unit_price": [25.50, 30.00, 25.50],
        "revenue": [127.50, 300.00, 76.50],
        "cost": [76.50, 180.00, 45.90],
        "profit": [51.00, 120.00, 30.60],
        "profit_margin": [40.0, 40.0, 40.0]
    })

    test_dim_user = pd.DataFrame({
        "user_key": [1, 2],
        "id": [1, 2],
        "full_name": ["Test User 1", "Test User 2"],
        "username": ["testuser1", "testuser2"],
        "email": ["test1@example.com", "test2@example.com"],
        "email_domain": ["example.com", "example.com"],
        "phone": ["555-1234", "555-5678"],
        "website": ["test1.com", "test2.com"],
        "company_name": ["Test Corp", "Demo Inc"],
        "city": ["Adelaide", "Sydney"],
        "street": ["123 Test St", "456 Demo Ave"],
        "zipcode": ["5000", "2000"],
        "name_length": [11, 11]
    })

    csv_payload = {
        "dim_time": test_dim_time,
        "dim_product": test_dim_product,
        "dim_customer": test_dim_customer,
        "fact_sales": test_fact_sales
    }

    api_payload = {
        "dim_user": test_dim_user
    }

    load_to_database(csv_payload, api_payload)
