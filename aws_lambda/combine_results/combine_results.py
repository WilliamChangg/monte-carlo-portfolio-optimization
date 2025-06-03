import boto3
import pandas as pd
from io import StringIO
import json
import ast
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from math import exp
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
opt_dynamodb_table = "OptimalPortfolios"
num_simulations = 1000      # Number of scenarios per portfolio allocation
initial_portfolio_value = 100000  # Starting portfolio value in dollars
risk_free_rate = 0.01      # Risk-free rate for Sharpe Ratio (e.g., 1%)
num_portfolios = 1      # Number of different portfolio weight combinations to test
num_days = 252
run_date = datetime.now().strftime('%Y-%m-%d')

def write_opt_metadata_to_dynamodb(table_name, run_date, initial_portfolio_value, 
                                    min_vol_id, min_volatility, min_vol_returns, vol_ev, vol_weights,
                                    max_sharpe_id, max_sharpe, max_sharpe_returns, sharpe_ev, sharpe_weights, 
                                    num_days, status):
    try:
        table = dynamodb.Table(table_name)

        min_volatility_dec = Decimal(str(min_volatility))
        max_sharpe_dec = Decimal(str(max_sharpe))
        expected_max_sharpe_value = Decimal(str(sharpe_ev))
        expected_min_volatility_value = Decimal(str(vol_ev))
        vol_weights_dec = {k: Decimal(str(v)) for k, v in vol_weights.items()}
        sharpe_weights_dec = {k: Decimal(str(v)) for k, v in sharpe_weights.items()}

        table.put_item(Item={
            'SimulationID': "Optimal_Portfolios",
            'RunDate': run_date,
            'InitialValue': int(initial_portfolio_value),
            'MinVolatilityID': min_vol_id,
            'MinVolatility': min_volatility_dec,
            'MinVolatilityReturns': Decimal(str(min_vol_returns)),
            'ExpectedPortfolio_MinVolatility': expected_min_volatility_value,
            'Volatility_Weights': vol_weights_dec,
            'MaxSharpeID': max_sharpe_id,
            'MaxSharpe': max_sharpe_dec,
            'MaxSharpeReturns': Decimal(str(max_sharpe_returns)),
            'ExpectedPortfolio_MaxSharpe': expected_max_sharpe_value,
            'Sharpe_Weights': sharpe_weights_dec,
            'TimeHorizon': int(num_days),
            'Status': status
        })
    except Exception as e:
        print(f"Error writing to DynamoDB: {str(e)}")
        raise 

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    bucket = 'monte-carlo-raw-data-william-chang'
    prefix = 'processed_data/'

    combined_df = pd.DataFrame()

    try:
        for i in range(10):
            key = f'{prefix}sim_results_{i}.csv'
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(obj['Body'])
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        
        results_df = pd.DataFrame(combined_df)
        # Parse the 'weights' column from Python-style strings to dictionaries
        results_df['weights'] = results_df['weights'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

        max_sharpe_idx = results_df['sharpe_ratio'].idxmax()
        min_volatility_idx = results_df['volatility'].idxmin()
    
        max_sharpe_row = results_df.loc[max_sharpe_idx]
        min_vol_row = results_df.loc[min_volatility_idx]

        max_sharpe_id = max_sharpe_row['simulation_id']
        min_vol_id = min_vol_row['simulation_id']

        max_sharpe = max_sharpe_row['sharpe_ratio']
        min_vol = min_vol_row['volatility']

        max_sharpe_weights = max_sharpe_row['weights']
        min_vol_weights = min_vol_row['weights']
        
        if not isinstance(max_sharpe_weights, dict) or not isinstance(min_vol_weights, dict):
            raise ValueError(f"Weights are not in dictionary format after parsing: max_sharpe_weights={max_sharpe_weights}, min_vol_weights={min_vol_weights}")

        max_sharpe_returns = max_sharpe_row['returns']
        min_vol_returns = min_vol_row['returns']

        expected_max_sharpe_value = initial_portfolio_value * exp(max_sharpe_returns)
        expected_min_volatility_value = initial_portfolio_value * exp(min_vol_returns)

        write_opt_metadata_to_dynamodb(
            opt_dynamodb_table, run_date, initial_portfolio_value, 
            min_vol_id, min_vol, min_vol_returns, expected_min_volatility_value, min_vol_weights,
            max_sharpe_id, max_sharpe, max_sharpe_returns, expected_max_sharpe_value, max_sharpe_weights, 
            num_days, "Completed"
        ) 

        combined_df.to_csv('/tmp/sim_results_combined.csv', index=False)
        s3_client.upload_file('/tmp/sim_results_combined.csv', bucket, f'{prefix}sim_results_combined.csv')

        return {
            'statusCode': 200,
            'body': json.dumps(f"Processed data and stats uploaded to s3://{bucket}")
        }
    except Exception as e:
        error_msg = f"Error occurred: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps(error_msg)
        }