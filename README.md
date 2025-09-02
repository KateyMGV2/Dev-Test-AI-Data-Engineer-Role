# Dev-Test-AI-Data-Engineer-Role
This repository contains a complete workflow to ingest advertising spend data into BigQuery, compute key performance metrics (CAC and ROAS), and provide easy access for analysts.

# Ads Spend KPI Analysis

## 1. Project Objective
This project ingests advertising spend data into BigQuery, computes key performance metrics (CAC & ROAS), and exposes them in a simple way for analysts.

**KPIs:**
- **CAC (Customer Acquisition Cost)** = spend / conversions
- **ROAS (Return on Ad Spend)** = (conversions × 100) / spend

The analysis includes comparison between the last 30 days and the prior 30 days, showing absolute values and percentage changes.

---

## 2. Ingestion Workflow
The ingestion is handled using **n8n**, which:
1. Reads the CSV dataset from Google Drive.
2. Adds metadata (`load_date` and `source_file_name`).
3. Inserts the data into BigQuery table:  
   `ageless-wall-470818-h0.my_dataset.ads_spend`.

**Workflow:** `ingestion/n8n_workflow.json`

---

## 3. SQL Models

### a. KPI Computation (Last 30 Days vs Prior 30 Days)
```sql
-- File: sql_models/30 DAYS_CURRENT DAYS.sql
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

```

### Part 4 - Analyst Acces

SQL scripts with parameters

```sql
-- File: sql_models/PART 3- SQL SCRIPT PARAMETERS.sql
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

