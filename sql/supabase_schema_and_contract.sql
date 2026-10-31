-- ==============================================================================
-- E-Commerce Relational Data Model & Data Contract (PostgreSQL / Supabase)
-- User -> Session -> Product -> Event Mimarisi
-- Veritabanı Seviyesinde CHECK Constraint ile Veri Kalitesi Sözleşmesi
-- ==============================================================================

-- 1. ENUM TANIMI
CREATE TYPE event_type_enum AS ENUM (
    'PRODUCT_LIST_VIEW',
    'SEARCH',
    'PRODUCT_DETAIL_VIEW',
    'ADD_TO_CART',
    'PURCHASE'
);

-- 2. TABLO TANIMLARI
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE products (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price > 0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE sessions (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    ended_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_id BIGINT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    event_type event_type_enum NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    product_id BIGINT REFERENCES products(id) ON DELETE SET NULL,
    search_query VARCHAR(255),
    quantity INT CHECK (quantity IS NULL OR quantity > 0),
    price NUMERIC(10, 2) CHECK (price IS NULL OR price > 0),

    -- ============================================================
    -- DATA CONTRACT (CHECK CONSTRAINTS)
    -- ============================================================
    -- Kural 1: SEARCH -> search_query dolu olmalı, product_id, quantity, price boş olmalı
    CONSTRAINT check_search_contract CHECK (
        event_type != 'SEARCH' OR (
            search_query IS NOT NULL AND
            product_id IS NULL AND
            quantity IS NULL AND
            price IS NULL
        )
    ),

    -- Kural 2: PRODUCT_DETAIL_VIEW -> product_id dolu olmalı, search_query, quantity, price boş olmalı
    CONSTRAINT check_detail_view_contract CHECK (
        event_type != 'PRODUCT_DETAIL_VIEW' OR (
            product_id IS NOT NULL AND
            search_query IS NULL AND
            quantity IS NULL AND
            price IS NULL
        )
    ),

    -- Kural 3: ADD_TO_CART -> product_id ve quantity dolu olmalı, search_query boş olmalı
    CONSTRAINT check_add_to_cart_contract CHECK (
        event_type != 'ADD_TO_CART' OR (
            product_id IS NOT NULL AND
            quantity IS NOT NULL AND
            search_query IS NULL
        )
    ),

    -- Kural 4: PURCHASE -> product_id, quantity ve price dolu olmalı, search_query boş olmalı
    CONSTRAINT check_purchase_contract CHECK (
        event_type != 'PURCHASE' OR (
            product_id IS NOT NULL AND
            quantity IS NOT NULL AND
            price IS NOT NULL AND
            search_query IS NULL
        )
    ),

    -- Kural 5: PRODUCT_LIST_VIEW -> product_id, search_query, quantity ve price boş olmalı
    CONSTRAINT check_list_view_contract CHECK (
        event_type != 'PRODUCT_LIST_VIEW' OR (
            product_id IS NULL AND
            search_query IS NULL AND
            quantity IS NULL AND
            price IS NULL
        )
    )
);

-- 3. PERFORMANS İNDEKSLERİ
CREATE INDEX idx_events_user_id ON events(user_id);
CREATE INDEX idx_events_session_id ON events(session_id);
CREATE INDEX idx_events_type_timestamp ON events(event_type, timestamp);

-- ==============================================================================
-- DOĞRULAMA VE TEST SENARYOLARI (11 Geçerli Event, 5 Negatif Test)
-- ==============================================================================

-- Örnek Başarılı Kayıt:
-- INSERT INTO events (user_id, session_id, event_type, timestamp, product_id, quantity, price)
-- VALUES (1, 1, 'PURCHASE', NOW(), 43, 2, 1183.79);

-- Negatif Test Örneği (CHECK Constraint tarafından reddedilmeli):
-- INSERT INTO events (user_id, session_id, event_type, timestamp)
-- VALUES (1, 1, 'PURCHASE', NOW()); -- product_id, quantity, price eksik olduğu için REDDEDİLİR!

