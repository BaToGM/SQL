# Set up feedback system
from learntools.core import binder
binder.bind(globals())
from learntools.sql.ex4 import *
print("Setup Complete")

from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "world_bank_intl_education" dataset
dataset_ref = client.dataset("world_bank_intl_education", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "international_education" table
table_ref = dataset_ref.table("international_education")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "international_education" table
client.list_rows(table, max_results=5).to_dataframe()

# Which countries spend the largest fraction of GDP on education?
# To answer this question, consider only the rows in the dataset corresponding to indicator code SE.XPD.TOTL.GD.ZS, and write a query that returns the average value in the value column for each country in the dataset between the years 2010-2017 (including 2010 and 2017 in the average).
# Requirements:
# Your results should have the country name rather than the country code. You will have one row for each country.
# The aggregate function for average is AVG(). Use the name avg_ed_spending_pct for the column created by this aggregation.
# Order the results so the countries that spend the largest fraction of GDP on education show up first.
country_spend_pct_query = """
                          SELECT country_name, AVG(value) as avg_ed_spending_pct
                          FROM `bigquery-public-data.world_bank_intl_education.international_education`
                          WHERE indicator_code='SE.XPD.TOTL.GD.ZS' AND year>=2010 AND year<=2017
                          GROUP BY country_name
                          ORDER BY avg_ed_spending_pct DESC
                          """


# The last question started by telling you to focus on rows with the code SE.XPD.TOTL.GD.ZS. But how would you find more interesting indicator codes to explore?
# There are 1000s of codes in the dataset, so it would be time consuming to review them all. But many codes are available for only a few countries. When browsing the options for different codes, you might restrict yourself to codes that are reported by many countries.
# Write a query below that selects the indicator code and indicator name for all codes with at least 175 rows in the year 2016.
# Requirements:
# You should have one row for each indicator code.
# The columns in your results should be called indicator_code, indicator_name, and num_rows.
# Only select codes with 175 or more rows in the raw database (exactly 175 rows would be included).
# To get both the indicator_code and indicator_name in your resulting DataFrame, you need to include both in your SELECT statement (in addition to a COUNT() aggregation). This requires you to include both in your GROUP BY clause.
# Order from results most frequent to least frequent.

# Your code goes here
code_count_query =  """
SELECT indicator_code, indicator_name, COUNT(*) as num_rows
FROM `bigquery-public-data.world_bank_intl_education.international_education`
WHERE year = 2016
GROUP BY indicator_code, indicator_name
HAVING COUNT(*) >= 175
ORDER BY COUNT(*) DESC
"""

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
country_spend_pct_query_job = client.query(country_spend_pct_query, job_config=safe_config)
code_count_query_job = client.query(code_count_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
country_spending_results = country_spend_pct_query_job.to_dataframe()
code_count_results = code_count_query_job.to_dataframe()
# View top few rows of results
print(country_spending_results.head())
print(code_count_results.head())