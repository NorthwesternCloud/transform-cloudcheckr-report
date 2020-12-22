#!/usr/bin/env python

import csv
import decimal
import io
import re
import sys
from google.cloud import storage

BUCKET_NAME = 'northwestern-it-report-data'

def transform(infile, outfile):

    fieldnames = ['Date', 'Service', 'Account', 'Friendly Name', 'Description',
            'Cost']

    reader = csv.DictReader(infile, fieldnames=fieldnames)

    w = csv.DictWriter(outfile, fieldnames)
    w.writerow(dict(zip(fieldnames, fieldnames)))

    for row in reader:
        outputrow = transform_row(row)
        if outputrow:
            w.writerow(outputrow)

def transform_row(row):

    # valid records start with mm-yyyy
    if not re.match('\d{2}-\d{4}', row['Date']):
        return None

    # only include records that cost at least $0.01
    try:
        cost = decimal.Decimal(row['Cost'])
    except (TypeError, decimal.InvalidOperation) as e:
        return None

    if cost < 0.01:
        return None

    output = {
        'Date': format_date(row['Date']),
        'Service': row['Service'],
        'Account': row['Account'],
        'Friendly Name': row['Friendly Name'],
        'Description': row['Description'],
        'Cost': str(round(cost, 2))
    }
    return output

def format_date(d):
    m, y = d.split('-')
    return f"{y}-{m}-01"

def main():
    cloudcheckr_data = sys.argv[1]

    outfile = io.StringIO()
    with open(cloudcheckr_data, 'r') as infile:
        transform(infile, outfile)

    outfile.seek(0)

    print(outfile.read())


def download_data(bucket_name, object_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(object_name)
    return blob.download_as_bytes().decode('utf-8')

def upload_data(bucket_name, object_name, f):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    blob.upload_from_string(f.read())

def handle_gcs(event, context):

    if not (event['bucket'] == BUCKET_NAME and event['name'].startswith('input/')):
        return

    print(f"Downloading {event['name']}")
    data = download_data(BUCKET_NAME, event['name'])
    infile = io.StringIO(data)
    outfile = io.StringIO()

    print("Beginning transformation")
    transform(infile, outfile)

    print("Uploading")
    outfile.seek(0)
    upload_data(BUCKET_NAME, 'CloudCheckr/latest.csv', outfile)

    print("Completed.")


if __name__ == '__main__':
    main()

