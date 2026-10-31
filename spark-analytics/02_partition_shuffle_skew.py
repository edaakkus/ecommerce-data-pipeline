"""
PySpark Performance Tuning: Partition Optimization, Broadcast Join & Data Skew Mitigation
- mapPartitions() ile partition başına düşen kayıt sayıları
- repartition() vs coalesce() karşılaştırması ve explain("formatted") çıktıları
- Broadcast Hash Join stratejisi
- 90.000 HOT_USER ile kontrollü Data Skew simülasyonu
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

def count_records_per_partition(iterator):
    yield sum(1 for _ in iterator)

def main():
    spark = (
        SparkSession.builder
        .appName("Ecommerce-Spark-Partition-Shuffle-Skew")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("1. PARTITION DAĞILIMI VE REPARTITION vs COALESCE")
    print("=" * 70)

    data_paths = [
        Path("../data-generation/events_5m.jsonl"),
        Path("../data-generation/events_100k.jsonl"),
        Path("data/events_sample.jsonl")
    ]
    data_path = next((p for p in data_paths if p.exists() and p.stat().st_size > 0), None)

    if not data_path:
        print("Uyarı: Veri dosyası bulunamadı.")
        return

    df = spark.read.json(str(data_path))
    initial_partitions = df.rdd.getNumPartitions()
    print(f"Başlangıç Partition Sayısı: {initial_partitions}")

    counts = df.rdd.mapPartitions(count_records_per_partition).collect()
    print(f"Partition Başına Kayıt Dağılımı: {counts}")

    print("\n--- repartition(24) Uygulanması (Full Shuffle) ---")
    df_repartitioned = df.repartition(24)
    print(f"Yeni Partition Sayısı: {df_repartitioned.rdd.getNumPartitions()}")
    print("repartition Fiziksel Plan Özeti (RoundRobinPartitioning & Exchange):")
    df_repartitioned.explain("formatted")

    print("\n--- coalesce(4) Uygulanması (No Shuffle) ---")
    df_coalesced = df.coalesce(4)
    print(f"Yeni Partition Sayısı: {df_coalesced.rdd.getNumPartitions()}")
    print("coalesce Fiziksel Plan Özeti:")
    df_coalesced.explain("formatted")

    print("\n" + "=" * 70)
    print("2. BROADCAST HASH JOIN İNCELEMESİ")
    print("=" * 70)

    # 100 ürünlük meta-veri tablosu
    products_data = [
        (i, f"Category_{(i % 5) + 1}", f"Brand_{(i % 10) + 1}", round(50.0 + (i * 15.5), 2))
        for i in range(1, 101)
    ]
    products_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("category", StringType(), False),
        StructField("brand", StringType(), False),
        StructField("list_price", DoubleType(), False)
    ])
    products_df = spark.createDataFrame(products_data, schema=products_schema)

    print(f"Örnek Ürün Tablosu Oluşturuldu: {products_df.count()} ürün")

    # Broadcast Hash Join
    joined_df = df.join(F.broadcast(products_df), on="product_id", how="inner")
    print(f"Join Sonucu Kayıt Sayısı: {joined_df.count():,}")
    print("\nBroadcast Join Fiziksel Planı (BroadcastHashJoin & BroadcastExchange):")
    joined_df.explain("formatted")

    print("\n" + "=" * 70)
    print("3. DATA SKEW SİMÜLASYONU VE KEY DAĞILIMI ANALİZİ")
    print("=" * 70)

    # Kontrollü test verisi: 100.000 kaydın 90.000'i tek bir kullanıcı anahtarında ('HOT_USER')
    print("100.000 kayıt oluşturuluyor (90.000 'HOT_USER', 10.000 'NORMAL_USER_i')...")
    skew_data = [("HOT_USER",) for _ in range(90_000)] + [(f"NORMAL_USER_{i}",) for i in range(10_000)]
    skew_df = spark.createDataFrame(skew_data, ["user_id"])

    print("groupBy('user_id').count() işlemiyle shuffle ve key yığılması inceleniyor:")
    skew_grouped = skew_df.groupBy("user_id").count()
    skew_grouped.show(5)

    print("Data Skew Senaryosunda Fiziksel Çalışma Planı:")
    skew_grouped.explain("formatted")

    spark.stop()

if __name__ == "__main__":
    main()

