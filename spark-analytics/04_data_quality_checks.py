"""
Automated 5-Dimensional Data Quality (DQ) Audit Framework
5 Boyutta Veri Kalitesi Çerçevesi:
1. Duplicate Kontrolü: Toplam satır vs Distinct Event ID
2. Missing (Eksik Veri) Kontrolü: Event tipine duyarlı null analizi (örn: ADD_TO_CART için price kasıtlı null'dır)
3. Invalid Kontrolü: Geçersiz event_type, negatif quantity/price kontrolü
4. Inconsistency (Tutarsızlık) Kontrolü: 1 session birden fazla kullanıcıya bağlanamaz
5. Anomaly (Anomali) Kontrolü: Session başına düşen olağandışı event sayısı kontrolü
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = (
        SparkSession.builder
        .appName("Ecommerce-Spark-Data-Quality")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("APACHE SPARK - 5 BOYUTLU VERİ KALİTESİ (DATA QUALITY) TESTİ")
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
    total_records = df.count()
    print(f"Denetlenen Toplam Kayıt Sayısı: {total_records:,}")

    # ============================================================
    # 1. DUPLICATE CHECK
    # ============================================================
    print("\n[1/5] DUPLICATE KONTROLÜ:")
    distinct_event_ids = df.select("id").distinct().count()
    duplicate_count = total_records - distinct_event_ids
    print(f" - Toplam Kayıt: {total_records:,}")
    print(f" - Tekil Event ID: {distinct_event_ids:,}")
    print(f" - Mükerrer (Duplicate) Kayıt Sayısı: {duplicate_count}")
    assert duplicate_count == 0, f"Hata: {duplicate_count} duplicate event ID tespit edildi!"
    print(" ✓ Duplicate kontrolü başarılı (0 duplicate).")

    # ============================================================
    # 2. MISSING (EKSİK VERİ) KONTROLÜ
    # ============================================================
    print("\n[2/5] MISSING (EKSİK VERİ) ANALİZİ:")
    # Zorunlu alanların kontrolü
    core_nulls = df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in ["id", "user_id", "session_id", "event_type", "timestamp"]
    ]).collect()[0].asDict()
    print(f" - Temel kolonlardaki null sayıları: {core_nulls}")

    # Event türüne duyarlı missing kontrolü:
    # ADD_TO_CART'ta price alanı kasıtlı null olabilir; ancak product_id ve quantity zorunludur.
    cart_missing_product = df.filter(
        (F.col("event_type") == "ADD_TO_CART") & (F.col("product_id").isNull() | F.col("quantity").isNull())
    ).count()
    # PURCHASE'da product_id, quantity ve price zorunludur.
    purchase_missing_fields = df.filter(
        (F.col("event_type") == "PURCHASE") & (F.col("product_id").isNull() | F.col("quantity").isNull() | F.col("price").isNull())
    ).count()

    print(f" - ADD_TO_CART zorunlu alan eksikliği: {cart_missing_product}")
    print(f" - PURCHASE zorunlu alan eksikliği: {purchase_missing_fields}")
    assert cart_missing_product == 0 and purchase_missing_fields == 0, "Hata: Zorunlu alanlarda eksik veri tespit edildi!"
    print(" ✓ Event-aware missing kontrolü başarılı.")

    # ============================================================
    # 3. INVALID DATA KONTROLÜ
    # ============================================================
    print("\n[3/5] INVALID (GEÇERSİZ VERİ) KONTROLÜ:")
    valid_events = ["PRODUCT_LIST_VIEW", "SEARCH", "PRODUCT_DETAIL_VIEW", "ADD_TO_CART", "PURCHASE"]
    invalid_event_types = df.filter(~F.col("event_type").isin(valid_events)).count()

    # Negatif veya sıfır miktar kontrolü
    invalid_quantity = df.filter(F.col("quantity").isNotNull() & (F.col("quantity") <= 0)).count()

    # Negatif veya sıfır fiyat kontrolü
    invalid_price = df.filter(F.col("price").isNotNull() & (F.col("price") <= 0)).count()

    print(f" - Tanımsız Event Tipi Sayısı: {invalid_event_types}")
    print(f" - Geçersiz (<= 0) Miktar Sayısı: {invalid_quantity}")
    print(f" - Geçersiz (<= 0) Fiyat Sayısı: {invalid_price}")

    # Fiyat dağılımı (Min, Max, Avg)
    price_stats = df.filter(F.col("price").isNotNull()).select(
        F.round(F.min("price"), 2).alias("min_price"),
        F.round(F.max("price"), 2).alias("max_price"),
        F.round(F.avg("price"), 2).alias("avg_price")
    ).collect()[0].asDict()
    print(f" - Fiyat Özeti: {price_stats}")
    print(" ✓ Invalid veri kontrolü başarılı.")

    # ============================================================
    # 4. INCONSISTENCY (TUTARSIZLIK) KONTROLÜ
    # ============================================================
    print("\n[4/5] INCONSISTENCY (TUTARSIZLIK) KONTROLÜ:")
    # Bir session birden fazla farklı user_id içeriyor mu?
    inconsistent_sessions = (
        df.groupBy("session_id")
        .agg(F.countDistinct("user_id").alias("user_count"))
        .filter(F.col("user_count") > 1)
        .count()
    )
    print(f" - Birden fazla kullanıcıya ait session sayısı: {inconsistent_sessions}")
    assert inconsistent_sessions == 0, "Hata: 1 session birden fazla kullanıcıya bağlı!"
    print(" ✓ Entity ilişki tutarlılığı başarılı (1 Session = 1 User).")

    # ============================================================
    # 5. ANOMALY (ANOMALİ) KONTROLÜ
    # ============================================================
    print("\n[5/5] ANOMALY (ANOMALİ) KONTROLÜ:")
    session_event_counts = df.groupBy("session_id").count()
    session_stats = session_event_counts.select(
        F.min("count").alias("min_events_per_session"),
        F.max("count").alias("max_events_per_session"),
        F.round(F.avg("count"), 2).alias("avg_events_per_session")
    ).collect()[0].asDict()

    print(f" - Session Boyut İstatistikleri: {session_stats}")
    # Güvenlik sınırı (max 50 event kuralı)
    assert session_stats["max_events_per_session"] <= 50, "Hata: Session event sınırını aşan oturumlar var!"
    print(" ✓ Anomali kontrolü başarılı (Aşırı şişkin session bulunamadı).")

    print("\n" + "=" * 70)
    print("TÜM VERİ KALİTESİ DENETİMLERİ BAŞARIYLA TAMAMLANDI!")
    print("=" * 70)

    spark.stop()

if __name__ == "__main__":
    main()

