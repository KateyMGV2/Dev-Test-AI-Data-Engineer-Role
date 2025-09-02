WITH base AS (
  SELECT
    DATE(date) AS dt,
    SUM(spend) AS spend,
    SUM(conversions) AS conversions
  FROM `ageless-wall-470818-h0.my_dataset.ads_spend`
  GROUP BY dt
),

kpis AS (
  SELECT
    dt,
    spend,
    conversions,
    SAFE_DIVIDE(spend, conversions) AS cac,
    SAFE_DIVIDE(conversions * 100, spend) AS roas
  FROM base
),

-- último día con datos
last_day AS (
  SELECT MAX(dt) AS max_date FROM kpis
),

periods AS (
  SELECT
    DATE_SUB(max_date, INTERVAL 29 DAY) AS start_last_30,
    max_date AS end_last_30,
    DATE_SUB(max_date, INTERVAL 59 DAY) AS start_prior_30,
    DATE_SUB(max_date, INTERVAL 30 DAY) AS end_prior_30
  FROM last_day
),

agg AS (
  SELECT
    'last_30' AS period,
    SUM(spend) AS spend,
    SUM(conversions) AS conversions,
    SAFE_DIVIDE(SUM(spend), SUM(conversions)) AS cac,
    SAFE_DIVIDE(SUM(conversions) * 100, SUM(spend)) AS roas
  FROM kpis, periods
  WHERE dt BETWEEN periods.start_last_30 AND periods.end_last_30

  UNION ALL

  SELECT
    'prior_30' AS period,
    SUM(spend) AS spend,
    SUM(conversions) AS conversions,
    SAFE_DIVIDE(SUM(spend), SUM(conversions)) AS cac,
    SAFE_DIVIDE(SUM(conversions) * 100, SUM(spend)) AS roas
  FROM kpis, periods
  WHERE dt BETWEEN periods.start_prior_30 AND periods.end_prior_30
),

final AS (
  SELECT
    MAX(CASE WHEN period='last_30' THEN spend END) AS spend_last_30,
    MAX(CASE WHEN period='prior_30' THEN spend END) AS spend_prior_30,
    MAX(CASE WHEN period='last_30' THEN conversions END) AS conv_last_30,
    MAX(CASE WHEN period='prior_30' THEN conversions END) AS conv_prior_30,
    MAX(CASE WHEN period='last_30' THEN cac END) AS cac_last_30,
    MAX(CASE WHEN period='prior_30' THEN cac END) AS cac_prior_30,
    MAX(CASE WHEN period='last_30' THEN roas END) AS roas_last_30,
    MAX(CASE WHEN period='prior_30' THEN roas END) AS roas_prior_30
  FROM agg
)

SELECT
  spend_last_30,
  spend_prior_30,
  ROUND(SAFE_DIVIDE(spend_last_30 - spend_prior_30, spend_prior_30) * 100, 2) AS spend_delta_pct,
  conv_last_30,
  conv_prior_30,
  ROUND(SAFE_DIVIDE(conv_last_30 - conv_prior_30, conv_prior_30) * 100, 2) AS conv_delta_pct,
  ROUND(cac_last_30, 2) AS cac_last_30,
  ROUND(cac_prior_30, 2) AS cac_prior_30,
  ROUND(SAFE_DIVIDE(cac_last_30 - cac_prior_30, cac_prior_30) * 100, 2) AS cac_delta_pct,
  ROUND(roas_last_30, 2) AS roas_last_30,
  ROUND(roas_prior_30, 2) AS roas_prior_30,
  ROUND(SAFE_DIVIDE(roas_last_30 - roas_prior_30, roas_prior_30) * 100, 2) AS roas_delta_pct
FROM final;
