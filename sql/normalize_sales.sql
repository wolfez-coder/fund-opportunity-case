DROP VIEW IF EXISTS normalized_sales_enriched;
DROP VIEW IF EXISTS normalized_sales;
DROP VIEW IF EXISTS normalized_broker_decoder;
DROP VIEW IF EXISTS normalized_city_decoder;
DROP VIEW IF EXISTS normalized_category_decoder;

CREATE VIEW normalized_broker_decoder AS
SELECT DISTINCT
    printf('%.0f', CAST("Broker Decoder" AS REAL)) AS broker_decoder_key,
    "Broker Name" AS broker_name
FROM Decoder_file
WHERE "Broker Decoder" IS NOT NULL
  AND TRIM(CAST("Broker Decoder" AS TEXT)) <> '';

CREATE VIEW normalized_city_decoder AS
SELECT DISTINCT
    printf('%.0f', CAST("City Decoder" AS REAL)) AS city_decoder_key,
    "City Name" AS city_name
FROM Decoder_file
WHERE "City Decoder" IS NOT NULL
  AND TRIM(CAST("City Decoder" AS TEXT)) <> '';

CREATE VIEW normalized_category_decoder AS
SELECT DISTINCT
    printf('%.0f', CAST("MS Category Decoder" AS REAL)) AS ms_cat_decoder_key,
    "Morningstar Category" AS category_name
FROM Decoder_file
WHERE "MS Category Decoder" IS NOT NULL
  AND TRIM(CAST("MS Category Decoder" AS TEXT)) <> '';

CREATE VIEW normalized_sales AS
SELECT
    source_file,
    printf('%.0f', CAST(broker_decoder AS REAL)) AS broker_decoder_key,
    territory,
    printf('%.0f', CAST(city_decoder AS REAL)) AS city_decoder_key,
    printf('%.0f', CAST(ms_cat_decoder AS REAL)) AS ms_cat_decoder_key,
    printf('%05d', CAST(zip AS INTEGER)) AS zip,
    CAST(client AS REAL) AS client_amount,
    CAST(opportunity AS REAL) AS opportunity_amount
FROM (
    SELECT 'file1' AS source_file, * FROM file1
    UNION ALL
    SELECT 'file2' AS source_file, * FROM file2
    UNION ALL
    SELECT 'file3' AS source_file, * FROM file3
    UNION ALL
    SELECT 'file4' AS source_file, * FROM file4
);

CREATE VIEW normalized_sales_enriched AS
SELECT
    s.source_file,
    s.broker_decoder_key,
    b.broker_name,
    s.territory,
    s.city_decoder_key,
    c.city_name,
    s.ms_cat_decoder_key,
    d.category_name,
    s.zip,
    s.client_amount,
    s.opportunity_amount,
    CASE
        WHEN s.opportunity_amount > 0 THEN s.client_amount / s.opportunity_amount
        ELSE NULL
    END AS capture_ratio,
    CASE
        WHEN s.client_amount < 0 OR s.opportunity_amount < 0 THEN 1
        ELSE 0
    END AS has_negative_values
FROM normalized_sales s
LEFT JOIN normalized_broker_decoder b
    ON s.broker_decoder_key = b.broker_decoder_key
LEFT JOIN normalized_city_decoder c
    ON s.city_decoder_key = c.city_decoder_key
LEFT JOIN normalized_category_decoder d
    ON s.ms_cat_decoder_key = d.ms_cat_decoder_key;
