WITH transaction_data AS (
    SELECT
      data.customer_id,
      data.transaction_id,
      data.month_id,
      data.month_name,
      CEIL(GREATEST(nr_days_since_creation - 30, 0) / 30) * 30 AS min_cohort,
      GREATEST(CEIL(nr_days_since_creation / 30) * 30, 30) AS max_cohort,
      CEIL(GREATEST(nr_days_since_creation - 30, 0) / 30) * 30 || ' - ' || GREATEST(CEIL(nr_days_since_creation / 30) * 30, 30) AS cohort_interval
    FROM (
        SELECT
            cds.customer_id,
            tds.transaction_id,
            d.month_id,
            d.month_name,
            DATE_PART('day', transaction_date::TIMESTAMP - account_creation_date::TIMESTAMP) AS nr_days_since_creation
        FROM transactions_data_sample tds
        LEFT JOIN customer_data_sample cds ON cds.customer_id = tds.customer_id
        LEFT JOIN day d ON d.day_id = cds.account_creation_date
    ) data
)

SELECT
    cohort_data.month_name,
    cohort_data.cohort_interval,
    ROUND((cohort_data.nr_transactions::NUMERIC / NULLIF(total_customers.nr_total_customers, 2))::NUMERIC, 2) AS avg_nr_transactions_per_user,
    ROUND((100.0 * cohort_data.nr_cohort_customers / NULLIF(total_customers.nr_total_customers, 0))::NUMERIC, 2) AS churn_rate
FROM (
    SELECT
        DISTINCT
        t.month_id,
        t.month_name,
        t.cohort_interval,
        t.min_cohort,
        COUNT(DISTINCT t.transaction_id) AS nr_transactions,
        COUNT(DISTINCT t.customer_id) nr_cohort_customers
    FROM transaction_data t
    GROUP BY 1, 2, 3, 4
) cohort_data

LEFT JOIN (
    SELECT
        d.month_id,
        COUNT(DISTINCT customer_id) AS nr_total_customers
    FROM customer_data_sample cds
    LEFT JOIN day d ON d.day_id = cds.account_creation_date
    GROUP BY 1
) total_customers ON total_customers.month_id = cohort_data.month_id
ORDER BY cohort_data.month_id, cohort_data.min_cohort
;
