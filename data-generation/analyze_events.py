import json
import os
from collections import Counter, defaultdict
from datetime import datetime


FILES = [
    "events_100k.jsonl",
    "events_1m.jsonl",
    "events_5m.jsonl",
]


def analyze_file(filename):

    print("=" * 70)
    print(f"ANALİTİK VERİ KONTROLÜ: {filename}")
    print("=" * 70)

    event_count = 0

    event_types = Counter()
    user_sessions = defaultdict(set)
    session_events = Counter()
    session_users = {}

    daily_events = Counter()
    daily_users = defaultdict(set)

    product_views = Counter()

    first_timestamp = None
    last_timestamp = None

    with open(
        filename,
        "r",
        encoding="utf-8"
    ) as file:

        for line in file:

            event = json.loads(line)

            event_count += 1

            event_type = event["event_type"]
            user_id = event["user_id"]
            session_id = event["session_id"]

            event_types[event_type] += 1

            # ------------------------------------------------
            # USER -> SESSION
            # ------------------------------------------------

            user_sessions[user_id].add(
                session_id
            )

            session_events[session_id] += 1
            session_users[session_id] = user_id

            # ------------------------------------------------
            # GÜNLÜK DAĞILIM
            # ------------------------------------------------

            timestamp = datetime.fromisoformat(
                event["timestamp"]
            )

            date = timestamp.date()

            daily_events[date] += 1
            daily_users[date].add(user_id)

            if (
                first_timestamp is None
                or timestamp < first_timestamp
            ):
                first_timestamp = timestamp

            if (
                last_timestamp is None
                or timestamp > last_timestamp
            ):
                last_timestamp = timestamp

            # ------------------------------------------------
            # ÜRÜN POPÜLERLİĞİ
            # ------------------------------------------------

            product_id = event.get("product_id")

            if product_id is not None:
                product_views[product_id] += 1

    # ========================================================
    # GENEL BİLGİLER
    # ========================================================

    unique_users = len(user_sessions)
    unique_sessions = len(session_events)

    print()
    print("GENEL BİLGİLER")
    print("-" * 70)

    print(
        f"Toplam event       : {event_count:,}"
    )

    print(
        f"Unique kullanıcı   : {unique_users:,}"
    )

    print(
        f"Unique session     : {unique_sessions:,}"
    )

    print(
        f"Tarih aralığı      : "
        f"{first_timestamp} -> {last_timestamp}"
    )

    # ========================================================
    # EVENT DAĞILIMI
    # ========================================================

    print()
    print("EVENT TÜRÜ DAĞILIMI")
    print("-" * 70)

    for event_type, count in event_types.items():

        percentage = (
            count / event_count * 100
        )

        print(
            f"{event_type:<28}"
            f"{count:>10,} "
            f"(%{percentage:.2f})"
        )

    # ========================================================
    # SESSION / USER
    # ========================================================

    session_count_distribution = Counter(
        len(sessions)
        for sessions
        in user_sessions.values()
    )

    print()
    print("KULLANICI BAŞINA SESSION DAĞILIMI")
    print("-" * 70)

    for session_count in sorted(
        session_count_distribution
    ):

        count = (
            session_count_distribution[
                session_count
            ]
        )

        percentage = (
            count / unique_users * 100
        )

        print(
            f"{session_count} session"
            f"{count:>15,} kullanıcı "
            f"(%{percentage:.2f})"
        )

    # ========================================================
    # SESSION BAŞINA EVENT
    # ========================================================

    total_session_events = sum(
        session_events.values()
    )

    average_events_per_session = (
        total_session_events
        / unique_sessions
    )

    print()
    print("SESSION BAŞINA EVENT")
    print("-" * 70)

    print(
        f"Ortalama event/session : "
        f"{average_events_per_session:.2f}"
    )

    print(
        f"Minimum event/session  : "
        f"{min(session_events.values())}"
    )

    print(
        f"Maksimum event/session : "
        f"{max(session_events.values())}"
    )

    # ========================================================
    # GÜNLÜK EVENT DAĞILIMI
    # ========================================================

    print()
    print("GÜNLÜK EVENT DAĞILIMI")
    print("-" * 70)

    for date in sorted(daily_events):

        print(
            f"{date} | "
            f"event: {daily_events[date]:>8,} | "
            f"unique kullanıcı: "
            f"{len(daily_users[date]):>8,}"
        )

    # ========================================================
    # EN POPÜLER ÜRÜNLER
    # ========================================================

    print()
    print("EN ÇOK ETKİLEŞİM ALAN 10 ÜRÜN")
    print("-" * 70)

    for rank, (
        product_id,
        count
    ) in enumerate(
        product_views.most_common(10),
        start=1
    ):

        print(
            f"{rank:>2}. Ürün {product_id:<5}"
            f"{count:>10,} event"
        )

    print("=" * 70)
    print()


def main():

    for filename in FILES:

        if not os.path.exists(filename):

            print(
                f"Dosya bulunamadı: {filename}"
            )

            continue

        analyze_file(filename)


if __name__ == "__main__":
    main()