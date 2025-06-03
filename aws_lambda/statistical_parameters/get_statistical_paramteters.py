import json
import pandas as pd
import numpy as np
import boto3
from datetime import datetime

def lambda_handler(event, context):
    raw_bucket = 'monte-carlo-raw-data-william-chang'
    # Replace with the latest raw data file name or make dynamic
    raw_key = 'raw_data/20250601.csv'
    processed_bucket = 'monte-carlo-raw-data-william-chang' 
    returns_key = 'processed_data/portfolio_returns.csv'
    stats_key = 'processed_data/portfolio_stats.csv'
    s3_client = boto3.client('s3')

    try:
        # Read raw data from S3
        response = s3_client.get_object(Bucket=raw_bucket, Key=raw_key)
        data = pd.read_csv(response['Body'], index_col=0, parse_dates=True)
        print(f"Read raw data from s3://{raw_bucket}/{raw_key}")

        # Calculate daily returns (percentage change)
        returns = data.pct_change().dropna()
        print(f"Calculated daily returns for {len(returns)} trading days")

        # Calculate statistical parameters for Monte Carlo simulations
        # Annualize metrics assuming 252 trading days per year
        mean_returns = returns.mean() * 252  # Annualized mean returns
        volatility = returns.std() * np.sqrt(252)  # Annualized volatility
        covariance = returns.cov() * 252  # Annualized covariance matrix ()
        
        stats = pd.DataFrame({
            'MeanReturn_Annual': mean_returns,
            'Volatility_Annual': volatility
        })

        # Save daily returns to temporary file and upload to S3
        returns_file = '/tmp/portfolio_returns.csv'
        returns.to_csv(returns_file)
        s3_client.upload_file(returns_file, processed_bucket, returns_key)
        print(f"Daily returns uploaded to s3://{processed_bucket}/{returns_key}")

        # Save stats (mean returns and volatility) to temporary file and upload to S3
        stats_file = '/tmp/portfolio_stats.csv'
        stats.to_csv(stats_file)
        s3_client.upload_file(stats_file, processed_bucket, stats_key)
        print(f"Summary stats uploaded to s3://{processed_bucket}/{stats_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Processed data and stats uploaded to s3://{processed_bucket}")
        }
    except Exception as e:
        error_msg = f"Error occurred: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps(error_msg)
        }