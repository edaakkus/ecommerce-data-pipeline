import json
from collections import Counter, defaultdict
from pathlib import Path
from datetime import datetime


FILES = [
    str(Path(__file__).resolve().parent / "events_100k.jsonl"),
    str(Path(__file__).resolve().parent / "events_1m.jsonl"),
    str(Path(__file__).resolve().parent / "events_5m.jsonl"),
]

EVENT_TYPES = [
    "PRODUCT_LIST_VIEW",
    "SEARCH",
    "PRODUCT_DETAIL_VIEW",
    "ADD_TO_CART",
    "PURCHASE"
]

# Teorik Markov geçişleri
EXPECTED_TRANSITIONS = {
    "PRODUCT_LIST_VIEW": {
        "SEARCH": 0.30,
        "PRODUCT_DETAIL_VIEW": 0.50,
        "END": 0.20
    },
    "SEARCH": {
        "SEARCH": 0.15,
        "PRODUCT_DETAIL_VIEW": 0.65,
        "END": 0.20
    },
    "PRODUCT_DETAIL_VIEW": {
        "PRODUCT_DETAIL_VIEW": 0.20,
        "ADD_TO_CART": 0.08,
        "END": 0.72
    },
    "ADD_TO_CART": {
        "ADD_TO_CART": 0.10,
        "PRODUCT_DETAIL_VIEW": 0.25,
        "PURCHASE": 0.30,
        "END": 0.35
    }
}


def analyze_file(filename):
    print("=" * 70)
    print(f"ANALİTİK VERİ ANALİZİ: {filename}")
    print("=" * 70)

    path = Path(filename)

    if not path.exists():
        print(f"Dosya bulunamadı: {filename}")
        return

    total_events = 0
    event_counts = Counter()

    users = set()
    sessions = set()

    session_users = {}
    session_events = defaultdict(list)
    user_sessions = defaultdict(set)

    product_views = Counter()
    product_carts = Counter()
    product_purchases = Counter()

    search_queries = Counter()

    transitions = Counter()

    first_timestamp = None
    last_timestamp = None

    # JSONL streaming
    with open(path, "r", encoding="utf-8") as file:
        for line in file:
            event = json.loads(line)

            total_events += 1

            event_type = event["event_type"]
            user_id = event["user_id"]
            session_id = event["session_id"]

            event_counts[event_type] += 1

            users.add(user_id)
            sessions.add(session_id)

            session_users[session_id] = user_id
            session_events[session_id].append(event_type)
            user_sessions[user_id].add(session_id)

            timestamp = datetime.fromisoformat(event["timestamp"])

            if first_timestamp is None or timestamp < first_timestamp:
                first_timestamp = timestamp

            if last_timestamp is None or timestamp > last_timestamp:
                last_timestamp = timestamp

            # Ürün analizleri
            if event_type == "PRODUCT_DETAIL_VIEW":
                product_id = event.get("product_id")
                if product_id is not None:
                    product_views[product_id] += 1

            elif event_type == "ADD_TO_CART":
                product_id = event.get("product_id")
                if product_id is not None:
                    product_carts[product_id] += 1

            elif event_type == "PURCHASE":
                product_id = event.get("product_id")
                if product_id is not None:
                    product_purchases[product_id] += 1

            # Search analizi
            elif event_type == "SEARCH":
                query = event.get("search_query")
                if query:
                    search_queries[query] += 1

    # ------------------------------------------------------------
    # GENEL KPI
    # ------------------------------------------------------------

    print("\nGENEL KPI'LAR")
    print("-" * 70)

    print(f"Toplam event       : {total_events:,}")
    print(f"Unique kullanıcı   : {len(users):,}")
    print(f"Unique session     : {len(sessions):,}")

    avg_events_per_session = total_events / len(sessions)

    print(f"Event / session    : {avg_events_per_session:.2f}")
    print(f"Tarih aralığı      : {first_timestamp} -> {last_timestamp}")

    # ------------------------------------------------------------
    # EVENT DAĞILIMI
    # ------------------------------------------------------------

    print("\nEVENT TÜRÜ DAĞILIMI")
    print("-" * 70)

    for event_type in EVENT_TYPES:
        count = event_counts[event_type]
        percentage = count / total_events * 100

        print(
            f"{event_type:<28} "
            f"{count:>10,} "
            f"(%{percentage:.2f})"
        )

    # ------------------------------------------------------------
    # SESSION / USER
    # ------------------------------------------------------------

    session_distribution = Counter(
        len(session_set)
        for session_set in user_sessions.values()
    )

    print("\nKULLANICI BAŞINA SESSION DAĞILIMI")
    print("-" * 70)

    for session_count in sorted(session_distribution):
        count = session_distribution[session_count]
        percentage = count / len(users) * 100

        print(
            f"{session_count} session"
            f"{' ' * (12 - len(str(session_count)))}"
            f"{count:>10,} kullanıcı (%{percentage:.2f})"
        )

    # ------------------------------------------------------------
    # FUNNEL
    # ------------------------------------------------------------

    detail_sessions = 0
    cart_sessions = 0
    purchase_sessions = 0

    for event_list in session_events.values():
        event_set = set(event_list)

        if "PRODUCT_DETAIL_VIEW" in event_set:
            detail_sessions += 1

        if "ADD_TO_CART" in event_set:
            cart_sessions += 1

        if "PURCHASE" in event_set:
            purchase_sessions += 1

    session_count = len(sessions)

    print("\nSESSION BAZLI FUNNEL")
    print("-" * 70)

    detail_rate = detail_sessions / session_count * 100
    cart_rate = cart_sessions / session_count * 100
    purchase_rate = purchase_sessions / session_count * 100

    print(
        f"PRODUCT_DETAIL_VIEW "
        f"{detail_sessions:,}/{session_count:,} "
        f"(%{detail_rate:.1f})"
    )

    print(
        f"ADD_TO_CART          "
        f"{cart_sessions:,}/{session_count:,} "
        f"(%{cart_rate:.1f})"
    )

    print(
        f"PURCHASE             "
        f"{purchase_sessions:,}/{session_count:,} "
        f"(%{purchase_rate:.1f})"
    )

    # ------------------------------------------------------------
    # FUNNEL DÖNÜŞÜM ORANLARI
    # ------------------------------------------------------------

    print("\nFUNNEL DÖNÜŞÜM ORANLARI")
    print("-" * 70)

    if detail_sessions:
        view_to_cart = cart_sessions / detail_sessions * 100
    else:
        view_to_cart = 0

    if cart_sessions:
        cart_to_purchase = purchase_sessions / cart_sessions * 100
    else:
        cart_to_purchase = 0

    print(f"DETAIL -> CART       : %{view_to_cart:.2f}")
    print(f"CART -> PURCHASE     : %{cart_to_purchase:.2f}")
    print(f"SESSION -> PURCHASE  : %{purchase_rate:.2f}")

    # ------------------------------------------------------------
    # ÜRÜN ANALİZİ
    # ------------------------------------------------------------

    print("\nEN ÇOK GÖRÜNTÜLENEN 10 ÜRÜN")
    print("-" * 70)

    for i, (product, count) in enumerate(product_views.most_common(10), 1):
        print(f"{i:>2}. {product:<15} {count:>10,}")

    print("\nEN ÇOK SEPETE EKLENEN 10 ÜRÜN")
    print("-" * 70)

    for i, (product, count) in enumerate(product_carts.most_common(10), 1):
        print(f"{i:>2}. {product:<15} {count:>10,}")

    print("\nEN ÇOK SATIN ALINAN 10 ÜRÜN")
    print("-" * 70)

    for i, (product, count) in enumerate(product_purchases.most_common(10), 1):
        print(f"{i:>2}. {product:<15} {count:>10,}")

    # ------------------------------------------------------------
    # SEARCH ANALİZİ
    # ------------------------------------------------------------

    print("\nEN ÇOK KULLANILAN 10 SEARCH QUERY")
    print("-" * 70)

    for i, (query, count) in enumerate(search_queries.most_common(10), 1):
        print(f"{i:>2}. {query:<30} {count:>10,}")

    # ------------------------------------------------------------
    # MARKOV GEÇİŞLERİ
    # ------------------------------------------------------------

    print("\nMARKOV GEÇİŞ ANALİZİ")
    print("-" * 70)

    # Session içindeki ardışık event geçişleri
    for event_list in session_events.values():

        for i in range(len(event_list) - 1):
            source = event_list[i]
            target = event_list[i + 1]

            transitions[(source, target)] += 1

    for source, expected in EXPECTED_TRANSITIONS.items():

        outgoing = sum(
            count
            for (src, _), count in transitions.items()
            if src == source
        )

        if outgoing == 0:
            continue

        print(f"\n{source}")

        for target, expected_probability in expected.items():

            actual_count = transitions.get(
                (source, target),
                0
            )

            actual_probability = actual_count / outgoing

            print(
                f"  -> {target:<25}"
                f" teorik: %{expected_probability * 100:>5.1f}"
                f" | gerçek: %{actual_probability * 100:>5.1f}"
            )

    print("\n" + "=" * 70)
    print("ANALİZ TAMAMLANDI")
    print("=" * 70)


# ------------------------------------------------------------
# TÜM DOSYALARI ANALİZ ET
# ------------------------------------------------------------

for file in FILES:
    analyze_file(file)