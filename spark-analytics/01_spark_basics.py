"""
PySpark Batch Ingestion & Aggregation Engine
- Büyük ölçekli e-ticaret event verisinin (5M) Spark DataFrame ile okunması
- Driver / Executor, Partition mimarisinin gözlemlenmesi
- filter, select, groupBy, count dönüşüm ve aksiyonları
"""

import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = (
        SparkSession.builder
        .appName("Ecommerce-Spark-Basics")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("APACHE SPARK - 5M E-TİCARET EVENT ANALİZİ")
    print("=" * 70)

    # Veri yolu kontrolü (öncelik 5M, yoksa 100k veya sample)
    data_paths = [
        Path("../data-generation/events_5m.jsonl"),
        Path("data/events_5m.jsonl"),
        Path("../data-generation/events_100k.jsonl"),
        Path("data/events_sample.jsonl")
    ]
    data_path = next((p for p in data_paths if p.exists() and p.stat().st_size > 0), None)

    if not data_path:
        print("Uyarı: Veri dosyası bulunamadı. Lütfen data-generation adımını çalıştırın.")
        return

    print(f"Yüklenen Veri Kaynağı: {data_path.resolve()}")
    df = spark.read.json(str(data_path))

    print("\n--- 1. DataFrame Şeması ---")
    df.printSchema()

    total_count = df.count()
    num_partitions = df.rdd.getNumPartitions()
    print(f"\nToplam Event Sayısı: {total_count:,}")
    print(f"Spark Partition Sayısı: {num_partitions}")

    print("\n--- 2. Satın Alma (PURCHASE) Eventleri Filtreleme ---")
    purchase_df = df.filter(F.col("event_type") == "PURCHASE")
    purchase_count = purchase_df.count()
    print(f"Toplam PURCHASE Event Sayısı: {purchase_count:,}")

    print("\nSatın alma kayıtlarından örnek (user_id, product_id, price, quantity):")
    purchase_df.select("user_id", "product_id", "price", "quantity").show(5, truncate=False)

    print("\n--- 3. Event Türlerine Göre Dağılım (groupBy & count) ---")
    event_distribution = (
        df.groupBy("event_type")
        .count()
        .withColumn("percentage", F.round((F.col("count") / total_count) * 100, 2))
        .orderBy(F.col("count").desc())
    )
    event_distribution.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()

