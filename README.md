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
-- File: sql_models/kpi_computation.sql
WITH base AS (
  SELECT
    DATE(date) AS dt,
    SUM(spend) AS spend,
    SUM(conversions) AS conversions
  FROM `ageless-wall-470818-h0.my_dataset.ads_spend`
  WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
  GROUP BY dt
),
agg AS (
  SELECT
    CASE WHEN dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 'last_30' ELSE 'prev_30' END AS period,
    SUM(spend) AS spend,
    SUM(conversions) AS conversions
  FROM base
  GROUP BY period
),
metrics AS (
  SELECT
    period,
    spend,
    conversions,
    SAFE_DIVIDE(spend, conversions) AS CAC,
    SAFE_DIVIDE(conversions * 100, spend) AS ROAS
  FROM agg
)
SELECT
  f1.period AS metric_period,
  f1.spend,
  f1.conversions,
  f1.CAC,
  f1.ROAS,
  ROUND(((f1.CAC - f2.CAC) / f2.CAC) * 100, 2) AS CAC_delta_pct,
  ROUND(((f1.ROAS - f2.ROAS) / f2.ROAS) * 100, 2) AS ROAS_delta_pct
FROM metrics f1
JOIN metrics f2
  ON f1.period = 'last_30' AND f2.period = 'prev_30';

