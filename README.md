# CloudCheckr Cost Report Transformation

This repository contains a function to be deployed to Google Cloud Functions
which will transform a CloudCheckr cost report (uploaded to Google Cloud
Storage) to a CSV that will be re-uploaded to Google Cloud Storage and used by
BigQuery to generate reports of spending on cloud services.