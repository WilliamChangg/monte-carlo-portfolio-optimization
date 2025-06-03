import json
import yfinance as yf
import pandas as pd
import boto3
from datetime import datetime

def lambda_handler(event, context):
    assets = ['META', 'GM', 'NVDA', 'JPM', 'GAP', 'GLD', 'PLTR', 'SPY']
    start_date = '2020-01-01'
    end_date = '2025-05-23'
    s3_bucket = "monte-carlo-raw-data-william-chang"
    s3_key = f"raw_data/{datetime.now().strftime('%Y%m%d')}.csv"
    s3_client = boto3.client('s3')

    try:
        data = yf.download(assets, start=start_date, end=end_date)
        if isinstance(data.columns, pd.MultiIndex):
            close_data = data['Close']
            print("Extracted 'Close' data as adjusted closing prices.")
        else:
            close_data = data
            print("DataFrame does not have multi-level index. Using as-is.")

        if close_data.empty:
            raise ValueError("No data downloaded.")
        
        local_file = '/tmp/portfolio_data.csv'
        close_data.to_csv(local_file)
        s3_client.upload_file(local_file, s3_bucket, s3_key)
        print(f"Data uploaded to S3: s3://{s3_bucket}/{s3_key}")
        return {'statusCode': 200, 'body': json.dumps(f"Data uploaded to s3://{s3_bucket}/{s3_key}")}
    except Exception as e:
        error_msg = f"Error occurred: {str(e)}"
        print(error_msg)
        return {'statusCode': 500, 'body': json.dumps(error_msg)}