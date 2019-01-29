### SQL Query Samples
This repository contains SQL samples for various transformations on Google BigQuery.  
BigQuery SQL dialect is used   

* `adjust_data_transformation`
    * groups adjust events into sessions and creates unique session IDs
* `smoothed_moving_average`
    * calculates the return and volatility of stocks based on the J.P. Morgan Riskmetrics model using a smoothed moving average
* `price_analysis`
    * calculates basic statistics for the product prices in a category
    * inputs are a sample of products with price and category data
    * if a product belongs to multiple categories it will be used independently in every category analysis
