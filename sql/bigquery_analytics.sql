-- ==============================================================================
-- Google BigQuery Analytics Suite: Deduplication, KPI Funnel & Trends
-- Deduplication, KPI Hesaplamaları, Funnel Analizi, Ürün ve Arama Trendleri
-- ==============================================================================

-- 1. DUPLICATE KAYITLARIN TEMİZLENMESİ VE `events_clean` TABLOSUNUN OLUŞTURULMASI
-- Yükleme sırasında 5.466.387 satır arasından 5.000.002 benzersiz event ID çıkarılması
CREATE OR REPLACE TABLE `ecommerce_dw.events_clean` AS
SELECT * EXCEPT(row_num)
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp ASC) AS row_num
    FROM `ecommerce_dw.events_raw`
)
WHERE row_num = 1;


-- 2. TEMEL E-TİCARET KPI'LARI
SELECT 
    COUNT(1) AS total_events,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNTIF(event_type = 'PURCHASE') AS total_purchases,
    COUNT(DISTINCT CASE WHEN event_type = 'PURCHASE' THEN user_id END) AS purchasing_users,
    ROUND(SUM(CASE WHEN event_type = 'PURCHASE' THEN price * quantity ELSE 0 END), 2) AS total_revenue
FROM `ecommerce_dw.events_clean`;
-- Beklenen Çıktılar:
-- Total Events: 5.000.002
-- Unique Users: 1.487.371
-- Unique Sessions: 2.082.635
-- Total Purchases: 59.579
-- Purchasing Users: 51.819
-- Total Revenue: 173.928.447,87


-- 3. SESSION BAZLI DÖNÜŞÜM HUNİSİ (FUNNEL ANALİZİ)
WITH session_steps AS (
    SELECT 
        session_id,
        MAX(CASE WHEN event_type = 'PRODUCT_LIST_VIEW' THEN 1 ELSE 0 END) AS has_list_view,
        MAX(CASE WHEN event_type = 'PRODUCT_DETAIL_VIEW' THEN 1 ELSE 0 END) AS has_detail_view,
        MAX(CASE WHEN event_type = 'ADD_TO_CART' THEN 1 ELSE 0 END) AS has_cart,
        MAX(CASE WHEN event_type = 'PURCHASE' THEN 1 ELSE 0 END) AS has_purchase
    FROM `ecommerce_dw.events_clean`
    GROUP BY session_id
)
SELECT 
    COUNT(session_id) AS total_sessions,
    SUM(has_detail_view) AS detail_sessions,
    SUM(has_cart) AS cart_sessions,
    SUM(has_purchase) AS purchase_sessions,
    ROUND(SUM(has_detail_view) / COUNT(session_id) * 100, 2) AS list_to_detail_pct,
    ROUND(SUM(has_cart) / SUM(has_detail_view) * 100, 2) AS detail_to_cart_pct,
    ROUND(SUM(has_purchase) / SUM(has_cart) * 100, 2) AS cart_to_purchase_pct,
    ROUND(SUM(has_purchase) / COUNT(session_id) * 100, 2) AS overall_session_conversion_rate
FROM session_steps;
-- Beklenen Dönüşüm Oranı: ~%2,51


-- 4. ÜRÜN BAZLI PERFORMANS (ZIPF DAĞILIMI, HACİM VE GELİR KARŞILAŞTIRMASI)
-- Ürün 1 (en yüksek adet/etkileşim) vs Ürün 3 (en yüksek ciro)
SELECT 
    product_id,
    COUNTIF(event_type = 'PRODUCT_DETAIL_VIEW') AS total_detail_views,
    COUNTIF(event_type = 'ADD_TO_CART') AS total_carts,
    COUNTIF(event_type = 'PURCHASE') AS total_purchases,
    SUM(CASE WHEN event_type = 'PURCHASE' THEN quantity ELSE 0 END) AS units_sold,
    ROUND(SUM(CASE WHEN event_type = 'PURCHASE' THEN price * quantity ELSE 0 END), 2) AS total_revenue
FROM `ecommerce_dw.events_clean`
WHERE product_id IS NOT NULL
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 10;


-- 5. KULLANICI ARAMA TRENDLERİ (SEARCH QUERY ANALİZİ)
SELECT 
    search_query,
    COUNT(1) AS search_count,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT session_id) AS unique_sessions
FROM `ecommerce_dw.events_clean`
WHERE event_type = 'SEARCH'
GROUP BY search_query
ORDER BY search_count DESC
LIMIT 10;

