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
        natural_keys = {
            "dim_customer": "customer_id",
            "dim_product": "product_id",
            "dim_time": "date",
            "dim_user": "id"
        }

        key_col = natural_keys.get(table)

        if key_col and key_col in df.columns:
            existing = pd.read_sql_query(
                f"SELECT {key_col} FROM {table}",
                self.conn
            )

            if not existing.empty:
                df = df[~df[key_col].isin(existing[key_col])]

        if not df.empty:
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


def load_to_database(csv_transformed, api_transformed):
    """Load transformed data to database"""
    print("  → Loading data to database...")
    
    db = ContosoDatabase()
    
    try:
        db.connect()
        db.create_schema()
        
        print("\n  → Loading sales data...")
        start_time = datetime.now()
        
        dim_time_count = db.load_dimension(csv_transformed['dim_time'], 'dim_time')
        print(f"    ✓ Loaded {dim_time_count} time dimension records")
        
        dim_product_count = db.load_dimension(csv_transformed['dim_product'], 'dim_product')
        print(f"    ✓ Loaded {dim_product_count} product dimension records")
        
        dim_customer_count = db.load_dimension(csv_transformed['dim_customer'], 'dim_customer')
        print(f"    ✓ Loaded {dim_customer_count} customer dimension records")
        
        fact_sales_count = db.load_fact(csv_transformed['fact_sales'], 'fact_sales')
        print(f"    ✓ Loaded {fact_sales_count} sales fact records")
        
        csv_time = (datetime.now() - start_time).total_seconds()
        db.log_pipeline_run('LOAD', 'CSV', fact_sales_count, 'SUCCESS', exec_time=csv_time)
        
        print("\n  → Loading user data...")
        start_time = datetime.now()
        
        dim_user_count = db.load_dimension(api_transformed['dim_user'], 'dim_user')
        print(f"    ✓ Loaded {dim_user_count} user dimension records")
        
        api_time = (datetime.now() - start_time).total_seconds()
        db.log_pipeline_run('LOAD', 'API', dim_user_count, 'SUCCESS', exec_time=api_time)
        print("\n  → Generating summary statistics...")
        stats = db.get_summary_stats()
        
        print("\n  " + "="*58)
        print("  DATABASE SUMMARY")
        print("  " + "="*58)
        
        if 'sales' in stats:
            print(f"  Total Transactions:     {stats['sales']['total_transactions']:,}")
            print(f"  Total Revenue:          ${stats['sales']['total_revenue']:,.2f}")
            print(f"  Total Profit:           ${stats['sales']['total_profit']:,.2f}")
            print(f"  Avg Transaction Value:  ${stats['sales']['avg_transaction_value']:,.2f}")
            print(f"  Avg Profit Margin:      {stats['sales']['avg_profit_margin']:.2f}%")
        
        print(f"  Total Products:         {stats['total_products']}")
        print(f"  Total Customers:        {stats['total_customers']}")
        print(f"  Total Users:            {stats['total_users']}")
        print("  " + "="*58)
        
        print("\n  → Sales by Region:")
        print("  " + "-"*58)
        regions = db.get_sales_by_region()
        for region, trans, revenue, profit, avg_trans in regions:
            print(f"  {region:10s} | {trans:3d} trans | ${revenue:10,.2f} | ${profit:9,.2f}")
        print("  " + "-"*58)
        
        print(f"\n  ✓ Database location: {db.db_path}")
        print(f"  ✓ All data loaded successfully")
        export_database_summary(stats)
        
        return stats
        
    except Exception as e:
        db.log_pipeline_run('LOAD', 'ERROR', 0, 'FAILED', str(e))
        raise Exception(f"Database loading failed: {str(e)}")
    
    finally:
        db.close()


def export_database_summary(stats):
    """Export database summary for website"""
    import json
    from pathlib import Path
    
    output_dir = Path("site/data")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    summary = {
        "stage": "database",
        "status": "SUCCESS",
        "timestamp": datetime.now().isoformat(),
        "statistics": stats,
        "database_location": str(DB_PATH)
    }
    
    output_file = output_dir / "database.json"
    with open(output_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"  ✓ Exported database summary to: {output_file}")