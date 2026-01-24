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
