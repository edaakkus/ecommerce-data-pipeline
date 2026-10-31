"""
Markov Zinciri Tabanlı Sentetik E-Ticaret Event Üreticisi

Session bütünlüğünü koruyan streaming JSONL üretim scripti.

Üretim aşamaları:
100K → 1M → 5M event

(bu sürümde):
1. Kullanıcıların ilk session'ları artık tek bir güne değil,
   N_DAYS parametresiyle belirlenen bir tarih aralığına yayılıyor.
   (Önceki sürümde tüm kullanıcılar aynı ~10 saatlik pencereye
   düşüyor, bu da "günlük aktif kullanıcı" analizini anlamsız
   kılıyordu.)
2. Çıktı dosyasının adı artık TARGET_EVENTS değerine göre otomatik
   belirleniyor (events_100k.jsonl / events_1m.jsonl / events_5m.jsonl).
   Böylece farklı ölçeklerde art arda çalıştırıldığında bir önceki
   dosyanın üzerine yazılma riski ortadan kalkıyor.
"""

import random
import json
import itertools
import os
import time
from collections import Counter
from datetime import datetime, timedelta


# ============================================================
# PARAMETRELER
# ============================================================

random.seed(42)

# Çalıştırmadan önce bu değeri değiştir: 100_000 / 1_000_000 / 5_000_000
TARGET_EVENTS = 5_000_000

N_PRODUCTS = 300

MAX_SEARCH_REPEAT = 3
MAX_DETAIL_REPEAT = 4
MAX_CART_ITEMS = 3
MAX_EVENTS_PER_SESSION = 50

SESSION_TIMEOUT_MINUTES = 30
EVENT_GAP_RANGE_SECONDS = (30, 360)

SESSION_COUNT_VALUES = [1, 2, 3]
SESSION_COUNT_PROBS = [0.70, 0.20, 0.10]

# Kullanıcıların ilk session'larının yayılacağı gün aralığı.
# Günlük/haftalık analizlerin (analitik) anlamlı olabilmesi için
# veri tek bir güne değil, birden fazla güne dağıtılır.
N_DAYS = 30


# ============================================================
# MARKOV GEÇİŞ MATRİSİ
# ============================================================

TRANSITIONS = {
    "PRODUCT_LIST_VIEW": [
        ("SEARCH", 0.30),
        ("PRODUCT_DETAIL_VIEW", 0.50),
        ("END", 0.20)
    ],

    "SEARCH": [
        ("SEARCH", 0.15),
        ("PRODUCT_DETAIL_VIEW", 0.65),
        ("END", 0.20)
    ],

    "PRODUCT_DETAIL_VIEW": [
        ("PRODUCT_DETAIL_VIEW", 0.20),
        ("ADD_TO_CART", 0.08),
        ("END", 0.72)
    ],

    "ADD_TO_CART": [
        ("ADD_TO_CART", 0.10),
        ("PRODUCT_DETAIL_VIEW", 0.25),
        ("PURCHASE", 0.30),
        ("END", 0.35)
    ],
}


SEARCH_QUERIES = [
    "kablosuz kulaklık",
    "mekanik klavye",
    "gaming mouse",
    "akıllı saat",
    "bluetooth hoparlör",
    "laptop standı",
    "webcam",
    "usb hub",
    "telefon kılıfı",
    "powerbank",
]


# ============================================================
# ÜRÜN KATALOĞU - ZIPF DAĞILIMI
# ============================================================

def build_product_catalog(n=N_PRODUCTS):
    products = []

    for i in range(1, n + 1):
        products.append({
            "id": i,
            "name": f"Ürün {i}",
            "category": random.choice([
                "Elektronik",
                "Aksesuar",
                "Bilgisayar",
                "Telefon"
            ]),
            "price": round(random.uniform(50, 5000), 2),
        })

    # Zipf: düşük ID'li ürünler daha popüler
    weights = [
        1 / rank
        for rank in range(1, n + 1)
    ]

    return products, weights


def pick_product(products, weights, exclude=None):

    exclude = exclude or set()

    candidates = [
        (product, weight)
        for product, weight
        in zip(products, weights)
        if product["id"] not in exclude
    ]

    if not candidates:
        return None

    picked_products, picked_weights = zip(
        *candidates
    )

    return random.choices(
        picked_products,
        weights=picked_weights,
        k=1
    )[0]


# ============================================================
# GEÇİŞ SEÇİMİ
# ============================================================

def get_allowed_transitions(
    current,
    counters,
    cart_size
):

    options = TRANSITIONS[current][:]

    # SEARCH tekrar sınırı
    if (
        current == "SEARCH"
        and counters["SEARCH"] >= MAX_SEARCH_REPEAT
    ):
        options = [
            (event, probability)
            for event, probability in options
            if event != "SEARCH"
        ]

    # DETAIL tekrar sınırı
    if (
        current == "PRODUCT_DETAIL_VIEW"
        and counters["DETAIL"] >= MAX_DETAIL_REPEAT
    ):
        options = [
            (event, probability)
            for event, probability in options
            if event != "PRODUCT_DETAIL_VIEW"
        ]

    # Sepet maksimum kapasitesine ulaştıysa
    # yeni ürün eklenmesini engelle.
    if cart_size >= MAX_CART_ITEMS:
        options = [
            (event, probability)
            for event, probability in options
            if event != "ADD_TO_CART"
        ]

    total = sum(
        probability
        for _, probability in options
    )

    if total == 0:
        return [("END", 1.0)]

    return [
        (event, probability / total)
        for event, probability in options
    ]


def choose_next(
    current,
    counters,
    cart_size
):

    options = get_allowed_transitions(
        current,
        counters,
        cart_size
    )

    events, probabilities = zip(*options)

    return random.choices(
        events,
        weights=probabilities,
        k=1
    )[0]


# ============================================================
# TEK SESSION ÜRETİMİ
# ============================================================

def generate_session(
    user_id,
    session_id,
    start_time,
    products,
    weights,
    event_id_counter
):

    events = []

    current = "PRODUCT_LIST_VIEW"
    timestamp = start_time

    counters = {
        "SEARCH": 0,
        "DETAIL": 0
    }

    # product_id -> quantity
    cart = {}

    last_detail_product = None

    event_count = 0

    while (
        current != "END"
        and event_count < MAX_EVENTS_PER_SESSION
    ):

        event = {
            "id": next(event_id_counter),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": current,
            "timestamp": timestamp.isoformat(),
            "product_id": None,
            "search_query": None,
            "quantity": None,
            "price": None,
        }

        # ----------------------------------------------------
        # SEARCH
        # ----------------------------------------------------

        if current == "SEARCH":

            event["search_query"] = random.choice(
                SEARCH_QUERIES
            )

            counters["SEARCH"] += 1
            counters["DETAIL"] = 0

        # ----------------------------------------------------
        # PRODUCT DETAIL
        # ----------------------------------------------------

        elif current == "PRODUCT_DETAIL_VIEW":

            # Sepete eklenen üründen sonra DETAIL'e
            # geçiliyorsa farklı ürün seç.
            if last_detail_product in cart:
                exclude = set(cart.keys())
            else:
                exclude = set()

            product = pick_product(
                products,
                weights,
                exclude
            )

            last_detail_product = product["id"]

            event["product_id"] = product["id"]

            counters["DETAIL"] += 1
            counters["SEARCH"] = 0

        # ----------------------------------------------------
        # ADD TO CART
        # ----------------------------------------------------

        elif current == "ADD_TO_CART":

            if (
                last_detail_product is None
                or last_detail_product in cart
            ):

                product = pick_product(
                    products,
                    weights,
                    exclude=set(cart.keys())
                )

                last_detail_product = product["id"]

            # Quantity ADD_TO_CART sırasında belirlenir.
            # PURCHASE aynı değeri kullanır.
            quantity = random.choices(
                [1, 2, 3],
                weights=[0.7, 0.2, 0.1],
                k=1
            )[0]

            if last_detail_product not in cart:

                cart[last_detail_product] = {
                    "quantity": quantity
                }

            event["product_id"] = (
                last_detail_product
            )

            event["quantity"] = cart[
                last_detail_product
            ]["quantity"]

            counters["SEARCH"] = 0
            counters["DETAIL"] = 0

        # ----------------------------------------------------
        # PRODUCT LIST VIEW
        # ----------------------------------------------------

        elif current == "PRODUCT_LIST_VIEW":

            counters["SEARCH"] = 0
            counters["DETAIL"] = 0

        events.append(event)

        event_count += 1

        # PURCHASE terminal eventtir.
        if current == "PURCHASE":
            break

        next_event = choose_next(
            current,
            counters,
            len(cart)
        )

        gap = random.randint(
            *EVENT_GAP_RANGE_SECONDS
        )

        timestamp += timedelta(
            seconds=gap
        )

        current = next_event

    # ========================================================
    # PURCHASE EVENTLERİ
    # ========================================================

    if (
        events
        and events[-1]["event_type"] == "PURCHASE"
    ):

        # Placeholder PURCHASE kaldırılır.
        events.pop()

        product_lookup = {
            product["id"]: product
            for product in products
        }

        # Sepetteki her ürün için ayrı PURCHASE.
        # Aynı timestamp = aynı checkout.
        for product_id, cart_item in cart.items():

            events.append({
                "id": next(event_id_counter),
                "user_id": user_id,
                "session_id": session_id,
                "event_type": "PURCHASE",
                "timestamp": timestamp.isoformat(),
                "product_id": product_id,
                "search_query": None,
                "quantity": cart_item["quantity"],
                "price": product_lookup[
                    product_id
                ]["price"],
            })

    ended_at = (
        events[-1]["timestamp"]
        if events
        else start_time.isoformat()
    )

    return events, ended_at


# ============================================================
# SESSION SAYISI
# ============================================================

def assign_session_count():

    return random.choices(
        SESSION_COUNT_VALUES,
        weights=SESSION_COUNT_PROBS,
        k=1
    )[0]


# ============================================================
# HEDEF EVENT SAYISINA KADAR STREAMING ÜRETİM
# ============================================================

def generate_until_target(
    target_events,
    output_path
):

    products, weights = build_product_catalog()

    event_id_counter = itertools.count(1)
    session_id_counter = itertools.count(1)

    base_time = datetime(
        2026,
        8,
        10,
        9,
        0,
        0
    )

    total_events = 0
    total_sessions = 0
    total_users = 0

    type_counts = Counter()

    start_time = time.time()

    # Eventler doğrudan JSONL dosyasına yazılır.
    # Tüm eventler RAM'de tutulmaz.
    with open(
        output_path,
        "w",
        encoding="utf-8"
    ) as file:

        user_id = 1

        while total_events < target_events:

            total_users += 1

            n_sessions = assign_session_count()

            # DÜZELTME:
            # İlk session artık yalnızca tek bir ~10 saatlik
            # pencereye değil, N_DAYS günlük bir aralığa yayılıyor.
            current_time = (
                base_time
                + timedelta(
                    days=random.randint(0, N_DAYS - 1),
                    minutes=random.randint(0, 600)
                )
            )

            for _ in range(n_sessions):

                session_id = next(
                    session_id_counter
                )

                session_events, ended_at = (
                    generate_session(
                        user_id,
                        session_id,
                        current_time,
                        products,
                        weights,
                        event_id_counter
                    )
                )

                if not session_events:
                    continue

                # ------------------------------------------------
                # SESSION TAMAMLANDIKTAN SONRA YAZILIR.
                # Böylece session yarıda kesilmez.
                # ------------------------------------------------

                for event in session_events:

                    file.write(
                        json.dumps(
                            event,
                            ensure_ascii=False
                        ) + "\n"
                    )

                    type_counts[
                        event["event_type"]
                    ] += 1

                total_events += len(
                    session_events
                )

                total_sessions += 1

                # Yeni session en az 31 dakika sonra.
                gap_minutes = random.randint(
                    SESSION_TIMEOUT_MINUTES + 1,
                    SESSION_TIMEOUT_MINUTES + 2000
                )

                current_time = (
                    datetime.fromisoformat(
                        ended_at
                    )
                    + timedelta(
                        minutes=gap_minutes
                    )
                )

                # Hedef aşıldıysa mevcut session
                # zaten tamamen yazılmıştır.
                if total_events >= target_events:
                    break

            user_id += 1

    elapsed_time = (
        time.time() - start_time
    )

    file_size_bytes = os.path.getsize(
        output_path
    )

    file_size_mb = (
        file_size_bytes
        / (1024 * 1024)
    )

    return {
        "users": total_users,
        "sessions": total_sessions,
        "events": total_events,
        "event_counts": type_counts,
        "elapsed_seconds": elapsed_time,
        "file_size_mb": file_size_mb,
    }


# ============================================================
# FUNNEL HESAPLAMA
# ============================================================

def calculate_funnel(output_path):

    session_event_types = {}

    with open(
        output_path,
        "r",
        encoding="utf-8"
    ) as file:

        for line in file:

            event = json.loads(line)

            session_id = event[
                "session_id"
            ]

            session_event_types.setdefault(
                session_id,
                set()
            ).add(
                event["event_type"]
            )

    total_sessions = len(
        session_event_types
    )

    funnel = {}

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

    return total_sessions, funnel


# ============================================================
# ÇIKTI DOSYA ADININ OTOMATİK BELİRLENMESİ
# ============================================================

def resolve_output_path(target_events):
    """
    TARGET_EVENTS değerine göre okunabilir bir dosya adı üretir.
    Böylece 100K/1M/5M üretimleri farklı dosyalara yazılır ve
    bir önceki ölçeğin dosyasının üzerine yazılma riski ortadan kalkar.
    """
    known_labels = {
        100_000: "100k",
        1_000_000: "1m",
        5_000_000: "5m",
    }
    label = known_labels.get(target_events, f"{target_events}")
    return f"events_{label}.jsonl"


# ============================================================
# MAIN
# ============================================================

def main():

    output_path = resolve_output_path(TARGET_EVENTS)

    print("=" * 60)
    print("SENTETİK E-TİCARET EVENT ÜRETİMİ")
    print("=" * 60)

    print(
        f"Hedef event sayısı: "
        f"{TARGET_EVENTS:,}"
    )

    print(
        f"Çıktı dosyası: {output_path}"
    )

    print(
        "Session bütünlüğü: AKTİF"
    )

    print(
        "Streaming JSONL: AKTİF"
    )

    print()

    result = generate_until_target(
        TARGET_EVENTS,
        output_path
    )

    print("=" * 60)
    print("ÜRETİM SONUCU")
    print("=" * 60)

    print(
        f"Hedef event sayısı : "
        f"{TARGET_EVENTS:,}"
    )

    print(
        f"Üretilen event     : "
        f"{result['events']:,}"
    )

    print(
        f"Toplam kullanıcı   : "
        f"{result['users']:,}"
    )

    print(
        f"Toplam session     : "
        f"{result['sessions']:,}"
    )

    print()

    print("Event türü dağılımı:")

    for event_type in [
        "PRODUCT_LIST_VIEW",
        "SEARCH",
        "PRODUCT_DETAIL_VIEW",
        "ADD_TO_CART",
        "PURCHASE"
    ]:

        print(
            f"  {event_type}: "
            f"{result['event_counts'].get(event_type, 0):,}"
        )

    print()

    print(
        f"Üretim süresi     : "
        f"{result['elapsed_seconds']:.2f} saniye"
    )

    print(
        f"Dosya boyutu      : "
        f"{result['file_size_mb']:.2f} MB"
    )

    print()

    total_sessions, funnel = calculate_funnel(
        output_path
    )

    print(
        "Session bazlı funnel:"
    )

    for stage, (
        reached,
        percentage
    ) in funnel.items():

        print(
            f"  {stage}: "
            f"{reached:,}/{total_sessions:,} "
            f"(%{percentage:.1f})"
        )

    print()

    print(
        f"JSONL dosyası: "
        f"{os.path.abspath(output_path)}"
    )

    print("=" * 60)


if __name__ == "__main__":
    main()