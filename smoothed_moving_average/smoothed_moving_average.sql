/**
  stock_data table is assumed to contain follwing columns
    date > the stock price date
    stock_name > the name of the stock, such that the returns and volatilities of multiple stocks can be calculated at the same time
    current_price > stock price on day in date
    lambda > smoothing parameter (can be hardcoded in the query)
**/

WITH data AS (
    SELECT
        sd.date,
        DATE_DIFF(CURRENT_DATE(), date, DAY) AS date_reverse,
        stock_name,
        sd.current_price,
        SAFE_DIVIDE(
            sd.current_price - LAG(sd.current_price) OVER (PARTITION BY sd.security_name ORDER BY sd.date),
            LAG(sd.current_price) OVER (PARTITION BY sd.security_name ORDER BY sd.date)
        ) AS return_d,
        lambda
    FROM stock_data sd
)

SELECT
    d.date,
    d.stock_name,
    d.return_d AS return,
    --Starting the exponential moving average calculation for volatility
    --Check screenshot.png for theoretical details
    --Calculating the denominator
    (1 / SUM(POW(d.lambda, date_reverse)) OVER (PARTITION BY d.stock_name ORDER BY date))
    *
    --Calculating the numerator
    (
        SUM(
            POW(d.lambda, date_reverse) * (return * return)
        ) OVER (PARTITION BY d.stock_name ORDER BY date)
    ) AS volatility
FROM data d
