-- ==============================================================================
-- Google BigQuery Customer Segmentation: Recency, Frequency & Monetary Scoring
-- Recency, Frequency, Monetary Skorlaması ve Segment Ayrımı
-- ==============================================================================

WITH max_date_ref AS (
    -- Referans gün: Veri setindeki en son event tarihi
    SELECT DATE(MAX(timestamp)) AS max_date 
    FROM `ecommerce_dw.events_clean`
),

user_rfm_raw AS (
    SELECT 
        e.user_id,
        DATE_DIFF(ref.max_date, DATE(MAX(e.timestamp)), DAY) AS recency_days,
        COUNT(e.id) AS frequency,
        ROUND(SUM(e.price * e.quantity), 2) AS monetary
    FROM `ecommerce_dw.events_clean` e
    CROSS JOIN max_date_ref ref
    WHERE e.event_type = 'PURCHASE'
    GROUP BY e.user_id, ref.max_date
),

user_rfm_scores AS (
    SELECT 
        user_id,
        recency_days,
        frequency,
        monetary,
        -- Skorlama: Recency için gün sayısı az olan daha iyi (5 en yeni)
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        -- Frequency dağılımı 1 civarında yoğunlaştığı için pratik kural / ntile
        NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
    FROM user_rfm_raw
)

SELECT 
    user_id,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    CONCAT(CAST(r_score AS STRING), CAST(f_score AS STRING), CAST(m_score AS STRING)) AS rfm_score,
    CASE 
        WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2 THEN 'New Customers'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
        ELSE 'Other'
    END AS customer_segment
FROM user_rfm_scores;

-- ==============================================================================
-- SEGMENT DAĞILIMI ÖZET SORGUSU
-- Toplam Alıcı Kullanıcı: 51.819
-- Dağılım:
-- - Other: 20.304
-- - Lost: 18.090
-- - New Customers: 8.989
-- - At Risk: 4.058
-- - Loyal Customers: 369
-- - Champions: 9
-- ==============================================================================

