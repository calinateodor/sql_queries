/**
  Inner query assigns a unique session id to every event in the raw data table
  Outer query groups the events belonging to the same session
  Output schema column names to match GA API web sessions names
**/

SELECT
    -- Aggregates events with the same session id into a sessions table. Each row represents a session
    adj.unique_indentifier                                                                                        AS sessionId,
    MIN(adj.created_at_milli)                                                                                     AS visitStartTime,
    MIN(TIMESTAMP_SECONDS(adj.created_at_milli))                                                                  AS visit_ts,
    MIN(adj.event_created_at_date)                                                                                AS date,
    COUNT(
        IF(activity_kind = 'event' AND event_name NOT IN ('PRODUCT_VIEW', 'ADD_TO_CART', 'CHECKOUT', 'PURCHASE'), adj.event_name, NULL)
    )                                                                                                             AS pageviews,
    MAX(adj.created_at_milli) - MIN(adj.created_at_milli)                                                         AS timeOnSite,
    COUNT(DISTINCT adj.transaction_id)                                                                            AS transactions,
    MAX(COALESCE(adj.adwords_adgroup_id, 0))                                                                      AS adGroupId,
    MAX(COALESCE(adj.adwords_campaign_id, 0))                                                                     AS campaignId,
    COALESCE(MAX(adj.campaign_name), 'UNKNOWN')                                                                   AS campaign,
    'UNKNOWN'                                                                                                     AS medium,
    COALESCE(MAX(adj.network_name), 'UNKNOWN')                                                                    AS source,
    COALESCE(MAX(adj.adgroup_name), 'UNKNOWN')                                                                    AS ad_content,
    MAX(adj.os_name)                                                                                              AS operatingSystem,
    MAX(adj.device_type)                                                                                          AS deviceCategory,
    MAX(adj.os_name)                                                                                              AS platformOS,
    MAX(UPPER(adj.country))                                                                                       AS country,
    MAX(adj.transaction_id)                                                                                       AS transactionId,
    SUM(IF(adj.event_name = 'PRODUCT_VIEW', 1, 0))                                                                AS product_view,
    SUM(IF(adj.event_name = 'ADD_TO_CART', 1, 0))                                                                 AS product_add_to_cart,
    SUM(IF(adj.event_name = 'CHECKOUT', 1, 0))                                                                    AS product_checkout,
    SUM(IF(adj.event_name = 'PURCHASE', 1, 0))                                                                    AS product_success,
    MAX(adj.adid)                                                                                                 AS app_adid
FROM (
    SELECT
        IF(
            -- If activity is not event, start a new session
            -- If activity kind is event, then take the session id of the previous row that is not activity type event, i.e. take the identifier of the session start
            -- New session id is created by combining the unique device id (adid or gps_adid) with the event timestamp
            activity_kind != 'event',
            CONCAT(COALESCE(adid, gps_adid), created_at_milli),
            LAST_VALUE(IF(activity_kind != 'event', CONCAT(COALESCE(adid, gps_adid), created_at_milli), NULL) IGNORE NULLS) OVER (PARTITION BY COALESCE(adid, gps_adid) ORDER BY created_at_milli ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ) AS unique_indentifier,
        CAST(CAST(
            IF(
               activity_kind != 'event',
               created_at_milli,
               LAST_VALUE(  IF(activity_kind != 'event', created_at_milli, NULL) IGNORE NULLS) OVER (PARTITION BY COALESCE(adid, gps_adid) ORDER BY created_at_milli ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            )
        AS FLOAT64) AS INT64) AS created_at_milli,
        JSON_EXTRACT_SCALAR(partner_parameters, '$.transaction_id') AS transaction_id,
        event_name,
        currency,
        country,
        os_name,
        device_type,
        COALESCE(adid, gps_adid) AS adid,
        event_created_at_date,
        activity_kind,
        CAST(revenue_float AS FLOAT64) AS revenue_float,
        CAST(CAST(adwords_adgroup_id AS FLOAT64)AS INT64) AS adwords_adgroup_id,
        CAST(CAST(adwords_campaign_id AS FLOAT64)AS INT64) AS adwords_campaign_id,
        campaign_name,
        network_name,
        adgroup_name
    FROM adjust_raw_data
    WHERE TRUE
) adj
