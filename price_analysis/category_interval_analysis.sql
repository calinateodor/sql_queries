/**
  For evevery category calculates following KPIs:
    average_price - average price
    min_price - minimum price
    max_price - maximum price
    median_price - median price
    stddev_perc - standard deviation in percentages over the average price
    price_10_percentile - 10% percentile price
    price_90_percentile - 90% percentile price
    nr_products - the sample size of the analysis

**/

SELECT
  DISTINCT
  category,
  ROUND(AVG(price) OVER (PARTITION BY category), 2) AS average_price,
  ROUND(MIN(price) OVER (PARTITION BY category), 2) AS min_price,
  ROUND(MAX(price) OVER (PARTITION BY category), 2) AS max_price,
  ROUND(FIRST_VALUE(price) OVER (PARTITION BY category ORDER BY distance_05), 2) AS median_price,
  ROUND(100.0 * STDDEV(price) OVER (PARTITION BY category) / (AVG(price) OVER (PARTITION BY category)), 2) AS stddev_perc,
  -- Taking the first value ordered by the distance selects that price which is closest to the desired percentile
  ROUND(FIRST_VALUE(price) OVER (PARTITION BY category ORDER BY distance_01), 2) AS price_10_percentile,
  ROUND(FIRST_VALUE(price) OVER (PARTITION BY category ORDER BY distance_09), 2) AS price_90_percentile,
  count(DISTINCT product_id) OVER (PARTITION BY category) AS nr_products
FROM (
    SELECT
        product_id,
        price,
        category,
        -- PERCENT_RANK() returns the percntile of the price inside the category
        -- Calculating the distance between the percentile of the price and the desired percentile helps find the closes price to the desired value
        POW(0.5 - PERCENT_RANK() OVER (PARTITION BY category ORDER BY price), 2) AS distance_05,
        POW(0.1 - PERCENT_RANK() OVER (PARTITION BY category ORDER BY price), 2) AS distance_01,
        POW(0.9 - PERCENT_RANK() OVER (PARTITION BY category ORDER BY price), 2) AS distance_09
    FROM (
        SELECT
            product_id,
            price,
            category
        FROM product
    ) product
) p
