# first_etl_project
This Project aims to create an ETL Data Pipeline simulating Extracting customer data, using Python pandas to Transform the data, and then Load showing saving it as a CSV for now.

Pipeline tasks
1. Extract Data from Mockaroo
2. Upload raw data to Datalake Google Cloud Storage to ensure data integrity and robustness
3. Extract data from Google Cloud Storage
4. Transform Data through Python
5. Upload Transformed data to GCS

Transformed data will then be loaded to Cloud SQL using another workflow/Cloud Function