WITH base AS (
  SELECT
    DATE(date) AS dt,
    spend,
    conversions,
    conversions * 100 AS revenue
  FROM `ageless-wall-470818-h0.my_dataset.ads_spend`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
),

agg AS (
  SELECT
    CASE 
      WHEN dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 'last_30'
      ELSE 'prev_30'
    END AS period,
    SUM(spend) AS total_spend,
    SUM(conversions) AS total_conversions,
    SUM(revenue) AS total_revenue
  FROM base
  GROUP BY period
),

metrics AS (
  SELECT
    period,
    total_spend,
    total_conversions,
    total_revenue,
    SAFE_DIVIDE(total_spend, total_conversions) AS CAC,
    SAFE_DIVIDE(total_revenue, total_spend) AS ROAS
  FROM agg
),

final AS (
  SELECT
    m1.*,
    -- Valores de la otra ventana
    m2.CAC AS prev_CAC,
    m2.ROAS AS prev_ROAS,
    -- Deltas (% change)
    SAFE_DIVIDE(m1.CAC - m2.CAC, m2.CAC) * 100 AS CAC_delta_pct,
    SAFE_DIVIDE(m1.ROAS - m2.ROAS, m2.ROAS) * 100 AS ROAS_delta_pct
  FROM metrics m1
  LEFT JOIN metrics m2
    ON m1.period = 'last_30'
   AND m2.period = 'prev_30'
  WHERE m1.period = 'last_30'
)

SELECT * FROM final;
