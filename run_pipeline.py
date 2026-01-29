import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pipeline.ingest import ingest_csv, ingest_api
from pipeline.validate import validate_data
from pipeline.transform import transform_data
from storage.sqlite_loader import load_to_database


def print_header():
    print("=" * 60)
    print("CONTOSO ANALYTICS DATA PIPELINE")
    print("=" * 60)
    print(f"Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()


def print_stage_header(stage_num, stage_name):
    print(f"[{stage_num}/4] {stage_name}")
    print("-" * 60)


def print_footer(start_time):
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print()
    print("=" * 60)
    print("PIPELINE COMPLETED")
    print(f"Start: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End:   {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Time:  {duration:.2f} seconds")
    print("=" * 60)

from pathlib import Path
from datetime import datetime
import json


def export_summary_json(stats):
    """
    Export pipeline summary statistics for frontend consumption.
    """
    output_dir = Path("site/data")
    output_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline_status": "success",
        "metrics": {
            "total_transactions": stats.get("sales", {}).get("total_transactions", 0),
            "total_revenue": stats.get("sales", {}).get("total_revenue", 0),
            "total_profit": stats.get("sales", {}).get("total_profit", 0),
            "avg_transaction_value": stats.get("sales", {}).get("avg_transaction_value", 0),
            "avg_profit_margin": stats.get("sales", {}).get("avg_profit_margin", 0),
            "total_customers": stats.get("total_customers", 0),
            "total_products": stats.get("total_products", 0),
            "total_users": stats.get("total_users", 0),
        },
        "data_quality": {
            "completeness": stats.get("data_quality", {}).get("completeness"),
            "schema_validation": stats.get("data_quality", {}).get("schema_validation"),
            "duplicates_found": stats.get("data_quality", {}).get("duplicates_found"),
            "business_rule_compliance": stats.get("data_quality", {}).get("business_rule_compliance"),
        },
    }

    output_file = output_dir / "summary.json"
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(f"Summary written to {output_file}")
    return output_file

def main():
    start_time = datetime.now()

    try:
        print_header()

        print_stage_header(1, "INGESTION")
        csv_data = ingest_csv()
        api_data = ingest_api()
        print("Ingestion finished\n")

        print_stage_header(2, "VALIDATION")
        validated_csv = validate_data(csv_data, "csv")
        validated_api = validate_data(api_data, "api")
        print("Validation finished\n")

        print_stage_header(3, "TRANSFORMATION")
        transformed_csv = transform_data(validated_csv, "csv")
        transformed_api = transform_data(validated_api, "api")
        print("Transformation finished\n")

        print_stage_header(4, "LOADING")
        stats = load_to_database(transformed_csv, transformed_api)
        print("Load finished\n")

        print_footer(start_time)
        return 0

    except Exception as e:
        print()
        print("=" * 60)
        print("PIPELINE FAILED")
        print("=" * 60)
        print(str(e))
        print("=" * 60)

        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    code = main()
    sys.exit(code)
