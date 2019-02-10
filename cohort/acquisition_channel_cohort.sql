WITH transaction_data AS (
    SELECT
        data.customer_id,
        data.transaction_id,
        data.month_id,
        data.month_name,
        data.acquisition_channel_id,
        data.nr_days_since_last_transaction,
        COALESCE(data.transaction_flat_fee, 0) + COALESCE(data.transaction_percentual_fee * data.transaction_value, 0) AS transaction_revenue,
        data.cohort_number::NUMERIC
    FROM (
        SELECT
            cds.customer_id,
            tds.transaction_id,
            d.month_id,
            d.month_name,
            tds.transaction_value,
            cds.acquisition_channel_id,
            DATE_PART('day', '2018-12-31'::TIMESTAMP - MAX(transaction_date) OVER (PARTITION BY cds.customer_id)::TIMESTAMP) AS nr_days_since_last_transaction,
            ttds.transaction_flat_fee,
            ttds.transaction_percentual_fee,
            13 - to_char(account_creation_date, 'MM')::INT AS cohort_number -- only used to calculate the monthly churn as the exponent
        FROM transactions_data_sample tds
        LEFT JOIN customer_data_sample cds ON cds.customer_id = tds.customer_id
        LEFT JOIN acquisition_channel_data_sample acds ON acds.acquisition_channel_id = cds.acquisition_channel_id
        LEFT JOIN day d ON d.day_id = cds.account_creation_date
        LEFT JOIN transaction_type_data_sample ttds ON ttds.transaction_type_id = tds.transaction_type_id
    ) data
)

SELECT
    cohort_data.month_name,
    acquisition_channel_name,
    ROUND((100.0 * nr_active_customers / NULLIF(SUM(nr_active_customers) OVER (PARTITION BY month_name), 0))::NUMERIC, 2) AS percent_active_customers,
    --CLTV / CAC <=> (avg monthly revenue / monthly churn) / avg CAC
    ROUND(
       ((
           cohort_data.total_revenue / NULLIF(total_customers.nr_total_customers * cohort_data.cohort_number, 0) --avg monthly customer revenue
            /
            NULLIF(
                (
                    1.0::NUMERIC
                    -
                    POW(
                        cohort_data.nr_active_customers::NUMERIC / NULLIF(total_customers.nr_total_customers, 0)::NUMERIC,
                        1.0::NUMERIC / cohort_data.cohort_number::NUMERIC
                    ) :: NUMERIC
                ),
                0
            )
        )::NUMERIC-- monthly churn rate
        /
        NULLIF(total_customers.avg_customer_acquisition_cost, 0))::NUMERIC
    , 2) AS cltv_per_cac_ratio
FROM (
    SELECT
        t.month_id,
        t.month_name,
        t.acquisition_channel_id,
        COUNT(DISTINCT CASE WHEN t.nr_days_since_last_transaction < 30 THEN t.customer_id END) AS nr_active_customers,
        SUM(t.transaction_revenue) AS total_revenue,
        MAX(cohort_number) AS cohort_number
    FROM transaction_data t
    GROUP BY 1, 2, 3
) cohort_data

LEFT JOIN (
    SELECT
        d.month_id,
        cds.acquisition_channel_id,
        isd.acquisition_channel_name,
        COUNT(DISTINCT customer_id) AS nr_total_customers,
        AVG(customer_acquisition_cost) AS avg_customer_acquisition_cost
    FROM customer_data_sample cds
    LEFT JOIN day d ON d.day_id = cds.account_creation_date
    LEFT JOIN acquisition_channel_data_sample isd ON isd.acquisition_channel_id = cds.acquisition_channel_id
    GROUP BY 1, 2, 3
) total_customers ON total_customers.month_id = cohort_data.month_id AND total_customers.acquisition_channel_id = cohort_data.acquisition_channel_id
ORDER BY cohort_data.month_id, total_customers.acquisition_channel_name
;
