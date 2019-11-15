# Purpose
This data leak can help data analysts at Sparkify in analyzing songs played by users. Origanaly, the data resides on JSON files which are not efficient for querying information. We use Spark to process the data and save it in much more efficient data storage format (e.g. Parquet).

# ETL Pipeline
The ETL pipeline process extracts data from the JSON files in the S3 bucket, processes them using Spark, then loads them back into another S3 bucket in a Parquet format.


# How to Run the Python Scripts
First we need add our configuration properties in the `dl.cfg`. The `dl.cfg` file contains these attributes:
- AWS_ACCESS_KEY_ID: Your Amazon Web Services access key 
- AWS_SECRET_ACCESS_KEY: Your Amazon Web Services secret access key

Before running the ETL script, you need to have the `pyspark` library insalled.
You can easily do that using `pip install pyspark`.