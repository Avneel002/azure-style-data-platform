import pandas as pd
from datetime import datetime
from pathlib import Path
import json

LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(parents=True, exist_ok=True)


class ValidationReport:
    def __init__(self, source_type):
        self.source_type = source_type
        self.timestamp = datetime.now().isoformat()
        self.checks = []
        self.errors = []
        self.warnings = []
        self.initial_count = 0
        self.final_count = 0

    def add_check(self, check_name, status, details=""):
        self.checks.append({
            "check": check_name,
            "status": status,
            "details": details
        })

    def add_error(self, error):
        self.errors.append(error)

    def add_warning(self, warning):
        self.warnings.append(warning)

    def save(self):
        report = {
            "timestamp": self.timestamp,
            "source_type": self.source_type,
            "initial_record_count": self.initial_count,
            "final_record_count": self.final_count,
            "records_removed": self.initial_count - self.final_count,
            "checks": self.checks,
            "errors": self.errors,
            "warnings": self.warnings,
            "status": "FAILED" if self.errors else "PASSED"
        }

        log_file = LOGS_DIR / f"validation_{self.source_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(log_file, "w") as f:
            json.dump(report, f, indent=2)

        return report


def validate_csv_schema(df):
    """Validate CSV data has required columns."""
    required_columns = [
        "transaction_id", "date", "product_id", "customer_id",
        "quantity", "unit_price", "total_amount"
    ]
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return True


def validate_api_schema(df):
    """Validate API data has required columns."""
    required_columns = ["id", "name", "username", "email"]
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return True


def export_validation_summary():
    """Export validation summary to JSON file."""
    output_dir = Path("site/data")
    output_dir.mkdir(parents=True, exist_ok=True)

    validation_logs = sorted(LOGS_DIR.glob("validation_*.json"))
    
    if validation_logs:
        summaries = []
        for log_file in validation_logs[-2:]:
            with open(log_file, 'r') as f:
                summaries.append(json.load(f))
        
        summary = {
            "stage": "validation",
            "status": "SUCCESS",
            "timestamp": datetime.now().isoformat(),
            "validations": summaries
        }
    else:
        summary = {
            "stage": "validation",
            "status": "NOT_RUN",
            "timestamp": None,
            "validations": []
        }

    output_file = output_dir / "validation.json"
    with open(output_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    return summary


def validate_data(df, source_type):
    """
    Main validation function for CSV and API data.
    
    Args:
        df: pandas DataFrame to validate
        source_type: either 'csv' or 'api'
    
    Returns:
        Validated DataFrame
    """
    print(f"\nValidating {source_type} data")
    report = ValidationReport(source_type)
    report.initial_count = len(df)
    
    df_validated = df.copy()

    try:
      
        if source_type == "csv":
            validate_csv_schema(df_validated)
        elif source_type == "api":
            validate_api_schema(df_validated)
        report.add_check("Schema Validation", "PASSED")

        
        nulls = df_validated.isnull().sum()
        if nulls.any():
            null_dict = nulls[nulls > 0].to_dict()
            report.add_warning(f"Null values detected: {null_dict}")

           
            if source_type == "csv":
                before = len(df_validated)
                df_validated = df_validated.dropna(
                    subset=["transaction_id", "product_id", "customer_id"]
                )
                removed = before - len(df_validated)
                report.add_check(
                    "Null Handling",
                    "PASSED",
                    f"Removed {removed} rows with null key fields"
                )
            else:  
                if "phone" in df_validated.columns:
                    df_validated["phone"] = df_validated["phone"].fillna("N/A")
                if "website" in df_validated.columns:
                    df_validated["website"] = df_validated["website"].fillna("N/A")
                report.add_check("Null Handling", "PASSED", "Filled defaults for optional fields")
        else:
            report.add_check("Null Check", "PASSED", "No null values found")

       
        key = "transaction_id" if source_type == "csv" else "id"
        before = len(df_validated)
        df_validated = df_validated.drop_duplicates(subset=[key])
        removed = before - len(df_validated)
        report.add_check(
            "Deduplication",
            "PASSED",
            f"Removed {removed} duplicate records"
        )

       
        if source_type == "csv":
            df_validated["date"] = pd.to_datetime(df_validated["date"], errors="coerce")
            df_validated["quantity"] = pd.to_numeric(df_validated["quantity"], errors="coerce")
            df_validated["unit_price"] = pd.to_numeric(df_validated["unit_price"], errors="coerce")
            df_validated["total_amount"] = pd.to_numeric(df_validated["total_amount"], errors="coerce")

            before = len(df_validated)
            df_validated = df_validated.dropna(
                subset=["date", "quantity", "unit_price", "total_amount"]
            )
            removed = before - len(df_validated)
            if removed > 0:
                report.add_warning(f"Removed {removed} rows due to type conversion issues")
            report.add_check("Type Enforcement", "PASSED")
        else:  
            df_validated["id"] = pd.to_numeric(df_validated["id"], errors="coerce")
            before = len(df_validated)
            df_validated = df_validated.dropna(subset=["id"])
            removed = before - len(df_validated)
            if removed > 0:
                report.add_warning(f"Removed {removed} rows with invalid ID")
            report.add_check("Type Enforcement", "PASSED")

      
        if source_type == "csv":
            before = len(df_validated)
            df_validated = df_validated[df_validated["quantity"] > 0]
            df_validated = df_validated[df_validated["unit_price"] > 0]
            removed = before - len(df_validated)
            
            df_validated["calculated_total"] = (
                df_validated["quantity"] * df_validated["unit_price"]
            )
            df_validated["total_amount"] = df_validated["calculated_total"]
            df_validated = df_validated.drop(columns=["calculated_total"])
            
            report.add_check(
                "Business Rules",
                "PASSED",
                f"Removed {removed} rows with invalid quantity/price; recalculated totals"
            )
        else:  
            before = len(df_validated)
            df_validated = df_validated[
                df_validated["email"].str.contains("@", na=False)
            ]
            removed = before - len(df_validated)
            report.add_check(
                "Business Rules",
                "PASSED",
                f"Removed {removed} rows with invalid email"
            )

        report.final_count = len(df_validated)
        saved_report = report.save()
        
        print(f" Validation complete: {report.initial_count} records in → {report.final_count} records out")
        print(f"  Status: {saved_report['status']}")
        if report.warnings:
            print(f"  Warnings: {len(report.warnings)}")

  
        export_validation_summary()

        return df_validated

    except Exception as e:
        report.add_error(str(e))
        report.save()
        raise Exception(f"Validation failed for {source_type}: {str(e)}")


if __name__ == "__main__":
    print("=" * 60)
    print("TESTING DATA VALIDATION MODULE")
    print("=" * 60)

    print("\n--- CSV Validation Test ---")
    sample_csv = pd.DataFrame({
        "transaction_id": ["TXN001", "TXN002", "TXN002", "TXN003", "TXN004"],
        "date": ["2025-01-01", "2025-01-02", "2025-01-02", "2025-01-03", "2025-01-04"],
        "product_id": ["PROD001", "PROD002", "PROD002", "PROD003", None],
        "customer_id": ["CUST001", "CUST002", "CUST002", "CUST003", "CUST004"],
        "quantity": [5, 10, 10, -1, 3],
        "unit_price": [25.50, 30.00, 30.00, 15.00, 0],
        "total_amount": [127.50, 300.00, 300.00, -15.00, 0],
        "region": ["North", "South", "South", "East", "West"]
    })

    print("\nInput data:")
    print(sample_csv)
    
    validated_csv = validate_data(sample_csv, "csv")
    
    print("\nValidated data:")
    print(validated_csv)

    print("\n\n API Validation Test")
    sample_api = pd.DataFrame({
        "id": [1, 2, 2, 3],
        "name": ["John Doe", "Jane Smith", "Jane Smith", "Bob Johnson"],
        "username": ["johnd", "janes", "janes", "bobj"],
        "email": ["john@example.com", "jane@example.com", "jane@example.com", "invalid-email"],
        "phone": ["555-1234", None, None, "555-5678"],
        "website": ["john.com", "jane.com", "jane.com", None]
    })

    print("\nInput data:")
    print(sample_api)
    
    validated_api = validate_data(sample_api, "api")
    
    print("\nValidated data:")
    print(validated_api)

    print("\n" + "=" * 60)
    print("VALIDATION MODULE TEST COMPLETE")
    print("=" * 60)