SELECT
    ROUND(SUM(client_amount), 2) AS total_client,
    ROUND(SUM(opportunity_amount), 2) AS total_opportunity,
    ROUND(SUM(opportunity_amount - client_amount), 2) AS total_missed_opportunity,
    ROUND(100.0 * SUM(client_amount) / NULLIF(SUM(opportunity_amount), 0), 2) AS capture_pct
FROM normalized_sales_enriched;

SELECT
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN broker_name IS NULL THEN 1 END) AS missing_broker_name,
    COUNT(CASE WHEN city_name IS NULL THEN 1 END) AS missing_city_name,
    COUNT(CASE WHEN category_name IS NULL THEN 1 END) AS missing_category_name,
    COUNT(CASE WHEN has_negative_values = 1 THEN 1 END) AS negative_value_rows
FROM normalized_sales_enriched;

SELECT
    territory,
    ROUND(SUM(client_amount), 2) AS client,
    ROUND(SUM(opportunity_amount), 2) AS opportunity,
    ROUND(SUM(opportunity_amount - client_amount), 2) AS missed_opportunity,
    ROUND(100.0 * SUM(client_amount) / NULLIF(SUM(opportunity_amount), 0), 2) AS capture_pct
FROM normalized_sales_enriched
GROUP BY territory
ORDER BY missed_opportunity DESC
LIMIT 20;

SELECT
    broker_name,
    ROUND(SUM(client_amount), 2) AS client,
    ROUND(SUM(opportunity_amount), 2) AS opportunity,
    ROUND(SUM(opportunity_amount - client_amount), 2) AS missed_opportunity,
    ROUND(100.0 * SUM(client_amount) / NULLIF(SUM(opportunity_amount), 0), 2) AS capture_pct
FROM normalized_sales_enriched
GROUP BY broker_name
ORDER BY missed_opportunity DESC
LIMIT 20;

SELECT
    category_name,
    ROUND(SUM(client_amount), 2) AS client,
    ROUND(SUM(opportunity_amount), 2) AS opportunity,
    ROUND(SUM(opportunity_amount - client_amount), 2) AS missed_opportunity,
    ROUND(100.0 * SUM(client_amount) / NULLIF(SUM(opportunity_amount), 0), 2) AS capture_pct
FROM normalized_sales_enriched
GROUP BY category_name
ORDER BY missed_opportunity DESC
LIMIT 20;

SELECT
    city_name,
    ROUND(SUM(client_amount), 2) AS client,
    ROUND(SUM(opportunity_amount), 2) AS opportunity,
    ROUND(SUM(opportunity_amount - client_amount), 2) AS missed_opportunity,
    ROUND(100.0 * SUM(client_amount) / NULLIF(SUM(opportunity_amount), 0), 2) AS capture_pct
FROM normalized_sales_enriched
GROUP BY city_name
ORDER BY missed_opportunity DESC
LIMIT 20;

SELECT
    territory,
    category_name,
    ROUND(SUM(client_amount), 2) AS client,
    ROUND(SUM(opportunity_amount), 2) AS opportunity,
    ROUND(SUM(opportunity_amount - client_amount), 2) AS missed_opportunity,
    ROUND(100.0 * SUM(client_amount) / NULLIF(SUM(opportunity_amount), 0), 2) AS capture_pct
FROM normalized_sales_enriched
GROUP BY territory, category_name
HAVING SUM(opportunity_amount) > 1000000
ORDER BY capture_pct ASC, missed_opportunity DESC
LIMIT 25;

SELECT
    broker_name,
    category_name,
    ROUND(SUM(client_amount), 2) AS client,
    ROUND(SUM(opportunity_amount), 2) AS opportunity,
    ROUND(SUM(opportunity_amount - client_amount), 2) AS missed_opportunity,
    ROUND(100.0 * SUM(client_amount) / NULLIF(SUM(opportunity_amount), 0), 2) AS capture_pct
FROM normalized_sales_enriched
GROUP BY broker_name, category_name
HAVING SUM(opportunity_amount) > 1000000
ORDER BY capture_pct ASC, missed_opportunity DESC
LIMIT 25;

SELECT
    zip,
    city_name,
    territory,
    ROUND(SUM(client_amount), 2) AS client,
    ROUND(SUM(opportunity_amount), 2) AS opportunity,
    ROUND(SUM(opportunity_amount - client_amount), 2) AS missed_opportunity,
    ROUND(100.0 * SUM(client_amount) / NULLIF(SUM(opportunity_amount), 0), 2) AS capture_pct
FROM normalized_sales_enriched
GROUP BY zip, city_name, territory
HAVING SUM(opportunity_amount) > 500000
ORDER BY capture_pct ASC, missed_opportunity DESC
LIMIT 25;

SELECT
    territory,
    broker_name,
    city_name,
    category_name,
    zip,
    ROUND(client_amount, 2) AS client,
    ROUND(opportunity_amount, 2) AS opportunity,
    ROUND(opportunity_amount - client_amount, 2) AS missed_opportunity
FROM normalized_sales_enriched
WHERE opportunity_amount > 0
  AND client_amount = 0
ORDER BY opportunity_amount DESC
LIMIT 50;