##@start_date = "2025-01-01"
##@end_date = "2025-01-31"

DECLARE start_date DATE DEFAULT "2025-03-01";
DECLARE end_date DATE DEFAULT "2025-03-31";

WITH base AS (
  SELECT
    DATE(date) AS dt,
    SUM(spend) AS total_spend,
    SUM(conversions) AS total_conversions
  FROM `ageless-wall-470818-h0.my_dataset.ads_spend`
  WHERE DATE(date) BETWEEN start_date AND end_date
  GROUP BY dt
),
metrics AS (
  SELECT
    SUM(total_spend) AS spend,
    SUM(total_conversions) AS conversions,
    SAFE_DIVIDE(SUM(total_spend), SUM(total_conversions)) AS CAC,
    SAFE_DIVIDE(SUM(total_conversions) * 100, SUM(total_spend)) AS ROAS
  FROM base
)
SELECT * FROM metrics;
