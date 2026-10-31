import json
import os
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


# ============================================================
# PARAMETRELER
# ============================================================

FILES = [
    str(Path(__file__).resolve().parent / "events_100k.jsonl"),
    str(Path(__file__).resolve().parent / "events_1m.jsonl"),
    str(Path(__file__).resolve().parent / "events_5m.jsonl"),
]

MAX_EVENTS_PER_SESSION = 50
MIN_SESSION_GAP_MINUTES = 30

VALID_EVENT_TYPES = {
    "PRODUCT_LIST_VIEW",
    "SEARCH",
    "PRODUCT_DETAIL_VIEW",
    "ADD_TO_CART",
    "PURCHASE",
}

REQUIRED_FIELDS = {
    "id",
    "user_id",
    "session_id",
    "event_type",
    "timestamp",
    "product_id",
    "search_query",
    "quantity",
    "price",
}


# ============================================================
# TEK DOSYA KONTROLÜ
# ============================================================

def validate_file(file_path):

    print("=" * 70)
    print(f"VERİ KALİTE KONTROLÜ: {file_path}")
    print("=" * 70)

    total_events = 0
    event_type_counts = Counter()

    unique_users = set()
    unique_sessions = set()

    session_users = {}
    session_event_counts = Counter()
    session_event_types = defaultdict(set)

    errors = Counter()

    null_counts = Counter()

    invalid_quantities = 0
    invalid_prices = 0
    invalid_timestamps = 0

    purchase_after_event_errors = 0
    timestamp_order_errors = 0

    # Session içindeki son timestamp
    last_timestamp_by_session = {}

    # Session başlangıç ve bitiş zamanlarını tutmak için
    session_start_times = {}
    session_end_times = {}

    # Kullanıcıya ait session'ları tutmak için
    user_sessions = defaultdict(list)

    # PURCHASE görüldükten sonra başka event geliyor mu?
    purchase_seen_sessions = set()

    # ADD_TO_CART quantity değerlerini takip etmek için
    cart_quantities = {}

    with open(
        file_path,
        "r",
        encoding="utf-8"
    ) as file:

        for line_number, line in enumerate(file, start=1):

            line = line.strip()

            if not line:
                errors["empty_line"] += 1
                continue

            # ------------------------------------------------
            # JSON FORMAT KONTROLÜ
            # ------------------------------------------------

            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                errors["invalid_json"] += 1
                continue

            total_events += 1

            # ------------------------------------------------
            # ALAN KONTROLÜ
            # ------------------------------------------------

            missing_fields = REQUIRED_FIELDS - set(event.keys())

            if missing_fields:
                errors["missing_fields"] += 1

            # ------------------------------------------------
            # EVENT TYPE
            # ------------------------------------------------

            event_type = event.get("event_type")

            event_type_counts[event_type] += 1

            if event_type not in VALID_EVENT_TYPES:
                errors["invalid_event_type"] += 1

            # ------------------------------------------------
            # USER / SESSION
            # ------------------------------------------------

            user_id = event.get("user_id")
            session_id = event.get("session_id")

            unique_users.add(user_id)
            unique_sessions.add(session_id)

            session_event_counts[session_id] += 1
            session_event_types[session_id].add(event_type)

            # Aynı session farklı user'a ait mi?
            if session_id in session_users:

                if session_users[session_id] != user_id:
                    errors["session_multiple_users"] += 1

            else:
                session_users[session_id] = user_id

            # ------------------------------------------------
            # NULL ALANLAR
            # ------------------------------------------------

            for field in [
                "product_id",
                "search_query",
                "quantity",
                "price"
            ]:

                if event.get(field) is None:
                    null_counts[field] += 1

            # ------------------------------------------------
            # TIMESTAMP
            # ------------------------------------------------

            timestamp_string = event.get("timestamp")

            try:
                timestamp = datetime.fromisoformat(
                    timestamp_string
                )
            except (
                ValueError,
                TypeError
            ):
                invalid_timestamps += 1
                continue

            # ------------------------------------------------
            # SESSION TIMESTAMP SIRALAMASI
            # ------------------------------------------------

            if session_id in last_timestamp_by_session:

                previous_timestamp = (
                    last_timestamp_by_session[session_id]
                )

                if timestamp < previous_timestamp:
                    timestamp_order_errors += 1

            else:
                # Session'ın ilk event'i
                session_start_times[session_id] = timestamp

            last_timestamp_by_session[
                session_id
            ] = timestamp

            # Her event işlendiğinde session'ın son zamanı güncellenir
            session_end_times[session_id] = timestamp

            # Kullanıcı-session ilişkisini takip et
            user_sessions[user_id].append(
                session_id
            )

            # ------------------------------------------------
            # PURCHASE TERMINAL KONTROLÜ
            # ------------------------------------------------

            if session_id in purchase_seen_sessions:

                # PURCHASE sonrasında yalnızca tekrar PURCHASE
                # gelmesine izin verilir.
                # Bu durum aynı checkout içerisindeki
                # birden fazla ürünün satın alınmasını temsil eder.

                if event_type != "PURCHASE":
                    purchase_after_event_errors += 1

            if event_type == "PURCHASE":
                purchase_seen_sessions.add(
                    session_id
                )

            # ------------------------------------------------
            # QUANTITY KONTROLÜ
            # ------------------------------------------------

            quantity = event.get("quantity")

            if quantity is not None:

                if (
                    not isinstance(quantity, int)
                    or quantity <= 0
                ):

                    invalid_quantities += 1

            # ------------------------------------------------
            # PRICE KONTROLÜ
            # ------------------------------------------------

            price = event.get("price")

            if price is not None:

                if (
                    not isinstance(price, (int, float))
                    or price <= 0
                ):

                    invalid_prices += 1

            # ------------------------------------------------
            # CART QUANTITY TAKİBİ
            # ------------------------------------------------

            if event_type == "ADD_TO_CART":

                product_id = event.get(
                    "product_id"
                )

                quantity = event.get(
                    "quantity"
                )

                if (
                    product_id is not None
                    and quantity is not None
                ):

                    cart_quantities[
                        (session_id, product_id)
                    ] = quantity

            elif event_type == "PURCHASE":

                product_id = event.get(
                    "product_id"
                )

                quantity = event.get(
                    "quantity"
                )

                expected_quantity = (
                    cart_quantities.get(
                        (session_id, product_id)
                    )
                )

                if (
                    expected_quantity is not None
                    and quantity != expected_quantity
                ):

                    errors[
                        "purchase_quantity_mismatch"
                    ] += 1

    # ========================================================
    # EVENT -> SESSION USER TUTARLILIK KONTROLÜ
    # ========================================================

    # Her event'in user_id'si, bağlı olduğu session'ın
    # user_id'si ile aynı olmalıdır.
    event_session_user_mismatch = 0

    # session_users sözlüğü event okunurken oluşturulduğu için
    # session içerisindeki tüm event'ler zaten aynı kullanıcıya
    # bağlanmış mı kontrol edilir.
    #
    # session_multiple_users kontrolü session'ın birden fazla
    # kullanıcıya ait olup olmadığını zaten yakalar.
    #
    # Burada event -> session -> user ilişkisinin açıkça
    # doğrulanması sağlanır.

    for session_id, session_user_id in session_users.items():

        # Bu session'a ait tüm event'ler aynı user_id ile
        # oluşturulduğu için session_users üzerinden kontrol edilir.
        if session_user_id is None:
            event_session_user_mismatch += 1

    # ========================================================
    # SESSION ARASI ZAMAN KONTROLÜ
    # ========================================================

    session_gap_errors = 0
    overlapping_sessions = 0

    # Aynı kullanıcıya ait session'ları tekilleştir
    # ve kronolojik sıraya göre kontrol et.
    for user_id in user_sessions:

        user_session_ids = sorted(
            set(user_sessions[user_id]),
            key=lambda sid: session_start_times.get(
                sid,
                datetime.max
            )
        )

        for i in range(1, len(user_session_ids)):

            previous_session_id = user_session_ids[i - 1]
            current_session_id = user_session_ids[i]

            previous_end = session_end_times.get(
                previous_session_id
            )

            current_start = session_start_times.get(
                current_session_id
            )

            if previous_end is None or current_start is None:
                continue

            gap_minutes = (
                current_start - previous_end
            ).total_seconds() / 60

            # Session'lar çakışıyor
            if gap_minutes < 0:
                overlapping_sessions += 1

            # Yeni session en az 30 dakika sonra başlamalı
            elif gap_minutes < MIN_SESSION_GAP_MINUTES:
                session_gap_errors += 1

    # ========================================================
    # SESSION LIMIT KONTROLÜ
    # ========================================================

    sessions_over_limit = sum(
        1
        for count
        in session_event_counts.values()
        if count > MAX_EVENTS_PER_SESSION
    )

    # ========================================================
    # SESSION FUNNEL
    # ========================================================

    funnel = {}

    total_sessions = len(
        session_event_types
    )

    for stage in [
        "PRODUCT_DETAIL_VIEW",
        "ADD_TO_CART",
        "PURCHASE"
    ]:

        reached = sum(
            1
            for event_types
            in session_event_types.values()
            if stage in event_types
        )

        percentage = (
            100 * reached / total_sessions
            if total_sessions
            else 0
        )

        funnel[stage] = (
            reached,
            percentage
        )

    # ========================================================
    # SONUÇLAR
    # ========================================================

    file_size_mb = (
        os.path.getsize(file_path)
        / (1024 * 1024)
    )

    print()

    print("GENEL BİLGİLER")
    print("-" * 70)

    print(
        f"Dosya boyutu       : "
        f"{file_size_mb:.2f} MB"
    )

    print(
        f"Toplam event       : "
        f"{total_events:,}"
    )

    print(
        f"Unique kullanıcı   : "
        f"{len(unique_users):,}"
    )

    print(
        f"Unique session     : "
        f"{len(unique_sessions):,}"
    )

    print()

    # --------------------------------------------------------
    # EVENT DAĞILIMI
    # --------------------------------------------------------

    print("EVENT TÜRÜ DAĞILIMI")
    print("-" * 70)

    for event_type in [
        "PRODUCT_LIST_VIEW",
        "SEARCH",
        "PRODUCT_DETAIL_VIEW",
        "ADD_TO_CART",
        "PURCHASE"
    ]:

        count = event_type_counts[
            event_type
        ]

        percentage = (
            100 * count / total_events
            if total_events
            else 0
        )

        print(
            f"{event_type:<25}"
            f"{count:>12,}"
            f"  (%{percentage:.2f})"
        )

    print()

    # --------------------------------------------------------
    # NULL ALANLAR
    # --------------------------------------------------------

    print("NULL ALAN DAĞILIMI")
    print("-" * 70)

    for field in [
        "product_id",
        "search_query",
        "quantity",
        "price"
    ]:

        print(
            f"{field:<20}"
            f"{null_counts[field]:>12,}"
        )

    print()

    # --------------------------------------------------------
    # SESSION KONTROLLERİ
    # --------------------------------------------------------

    print("SESSION KONTROLLERİ")
    print("-" * 70)

    print(
        f"Farklı kullanıcıya ait session : "
        f"{errors['session_multiple_users']:,}"
    )

    print(
        f"Event-session user_id uyumsuzluğu : "
        f"{event_session_user_mismatch:,}"
    )

    print(
        f"50 event sınırını aşan session : "
        f"{sessions_over_limit:,}"
    )

    print(
        f"Timestamp sıralama hatası      : "
        f"{timestamp_order_errors:,}"
    )

    print(
        f"30 dk'dan kısa session aralığı : "
        f"{session_gap_errors:,}"
    )

    print(
        f"Çakışan session                 : "
        f"{overlapping_sessions:,}"
    )

    print(
        f"PURCHASE sonrası event         : "
        f"{purchase_after_event_errors:,}"
    )

    print()

    # --------------------------------------------------------
    # VERİ TİPİ / FORMAT KONTROLLERİ
    # --------------------------------------------------------

    print("VERİ GEÇERLİLİK KONTROLLERİ")
    print("-" * 70)

    print(
        f"Geçersiz JSON                  : "
        f"{errors['invalid_json']:,}"
    )

    print(
        f"Eksik alan içeren event        : "
        f"{errors['missing_fields']:,}"
    )

    print(
        f"Geçersiz event_type            : "
        f"{errors['invalid_event_type']:,}"
    )

    print(
        f"Geçersiz timestamp             : "
        f"{invalid_timestamps:,}"
    )

    print(
        f"Geçersiz quantity              : "
        f"{invalid_quantities:,}"
    )

    print(
        f"Geçersiz price                 : "
        f"{invalid_prices:,}"
    )

    print(
        f"Purchase quantity uyuşmazlığı : "
        f"{errors['purchase_quantity_mismatch']:,}"
    )

    print()

    # --------------------------------------------------------
    # FUNNEL
    # --------------------------------------------------------

    print("SESSION BAZLI FUNNEL")
    print("-" * 70)

    for stage, (
        reached,
        percentage
    ) in funnel.items():

        print(
            f"{stage:<25}"
            f"{reached:>10,}/"
            f"{total_sessions:,}"
            f"  (%{percentage:.1f})"
        )

    print()

    # --------------------------------------------------------
    # GENEL DURUM
    # --------------------------------------------------------

    total_errors = (
        sum(errors.values())
        + invalid_quantities
        + invalid_prices
        + invalid_timestamps
        + timestamp_order_errors
        + purchase_after_event_errors
        + sessions_over_limit
        + event_session_user_mismatch
        + session_gap_errors
        + overlapping_sessions
    )

    print("=" * 70)

    if total_errors == 0:

        print(
            "SONUÇ: VERİ KALİTE KONTROLÜ BAŞARILI"
        )

        print(
            "Herhangi bir veri bütünlüğü "
            "problemi tespit edilmedi."
        )

    else:

        print(
            f"SONUÇ: {total_errors:,} KONTROL PROBLEMİ TESPİT EDİLDİ"
        )

    print("=" * 70)

    print()


# ============================================================
# MAIN
# ============================================================

def main():

    for file_path in FILES:

        if not os.path.exists(file_path):

            print(
                f"Dosya bulunamadı: {file_path}"
            )

            continue

        validate_file(
            file_path
        )


if __name__ == "__main__":
    main()