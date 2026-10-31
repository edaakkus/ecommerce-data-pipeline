from datetime import datetime, timedelta
from pathlib import Path

try:
    from airflow.decorators import dag, task
except ImportError:
    from airflow.sdk import dag, task


DATA_FILE = Path("/opt/airflow/data/events_5m.jsonl")


@dag(
    dag_id="ecommerce_analytics_pipeline",
    start_date=datetime(2026, 8, 21),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "analytics", "production"],
)
def ecommerce_analytics_pipeline():

    @task(
    retries=2,
    retry_delay=timedelta(seconds=10),
    )
    def prepare_data():
        print("=" * 60)
        print("1. PREPARE DATA")
        print("=" * 60)

        print(f"Veri dosyası: {DATA_FILE}")

        if not DATA_FILE.exists():
            raise FileNotFoundError(
                f"5M event veri dosyası bulunamadı: {DATA_FILE}"
            )

        file_size_mb = DATA_FILE.stat().st_size / (1024 * 1024)

        print("Dosya bulundu.")
        print(f"Dosya boyutu: {file_size_mb:.2f} MB")

        if file_size_mb < 900:
            raise ValueError(
                f"Dosya boyutu beklenenden küçük: {file_size_mb:.2f} MB"
            )

        print("5M eventlik veri seti pipeline için hazır.")
        print("=" * 60)

        return str(DATA_FILE)

    @task(
    retries=2,
    retry_delay=timedelta(seconds=10),
    )
    def validate_data(file_path):
        print("=" * 60)
        print("2. VALIDATE DATA")
        print("=" * 60)

        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(
                f"Validation için veri dosyası bulunamadı: {file_path}"
            )

        print(f"Kontrol edilen dosya: {path}")

        total_events = 0

        with path.open("r", encoding="utf-8") as file:
            for line in file:
                if line.strip():
                    total_events += 1

        print(f"Toplam event: {total_events:,}")

        expected_events = 5_000_002

        if total_events != expected_events:
            raise ValueError(
                f"Event sayısı beklenenden farklı! "
                f"Beklenen: {expected_events:,}, "
                f"Bulunan: {total_events:,}"
            )

        print("Event sayısı doğrulandı.")

        print("Veri validation başarılı.")
        print("5M eventlik veri seti analiz için hazır.")
        print("=" * 60)

        return {
            "file_path": file_path,
            "total_events": total_events,
        }

    @task
    def analyze_data(validation_result):
        print("=" * 60)
        print("3. ANALYZE DATA")
        print("=" * 60)

        file_path = Path(validation_result["file_path"])
        total_events = validation_result["total_events"]

        print(f"Analiz edilen dosya: {file_path}")
        print(f"Toplam event: {total_events:,}")

        event_counts = {
            "PRODUCT_LIST_VIEW": 0,
            "SEARCH": 0,
            "PRODUCT_DETAIL_VIEW": 0,
            "ADD_TO_CART": 0,
            "PURCHASE": 0,
        }

        with file_path.open("r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue

                import json

                event = json.loads(line)
                event_type = event.get("event_type")

                if event_type in event_counts:
                    event_counts[event_type] += 1

        print("\nEVENT DAĞILIMI")
        print("-" * 40)

        for event_type, count in event_counts.items():
            percentage = (count / total_events) * 100

            print(
                f"{event_type:<25} "
                f"{count:>10,} "
                f"(%{percentage:.2f})"
            )

        print("\nAnaliz tamamlandı.")
        print("BigQuery SQL analizleri bir sonraki aşamada eklenecektir.")
        print("=" * 60)

        return event_counts

    file_path = prepare_data()

    validation_result = validate_data(file_path)

    analyze_data(validation_result)


ecommerce_analytics_pipeline()