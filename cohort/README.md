#Cohorts in PSQL
PSQL example implementation of 3 different cohorts, using FinTech transaction data as basis.  

##Content
* `day.sql` 
    * generates a day table  
    * has nicely formatted columns for months and weeks which make the other queries less complex  
* `time_interval_cohort.sql`  
    * Segments customers in monthly cohorts  
    * Measures the activity of every cohort in 30 days intervals from account creation date  
    * Metrics are **churn rate** (defined as no transaction in the 30 day interval) and **# transactions per user**  
* `industry_cohort.sql`  
    * Goal is to assess the customer base composition based on their industry and onboarding month   
    * Segments customers in monthly cohorts  
    * Measure the activity of each cohort grouped by their industry  
    * Metrics are **percentage of active customers** by industry and **avg number of transactions per month**  
* `acquisition_channel_cohort.sql`  
    * Similar to industry cohort
    * Segment customers based on onboarding month and acquisition channel
    * Metrics **CLTV to CAC Ratio** and **percentage of active customers**

## Sample Data
Randomly generated data. Only use to showcase the schema of the tables used in the queries  
