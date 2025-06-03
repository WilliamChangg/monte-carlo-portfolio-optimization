import numpy as np
import pandas as pd
from datetime import datetime
from math import exp
import boto3
import uuid
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import os

region = 'us-east-1'
s3_client = boto3.client('s3', region_name=region)
dynamodb = boto3.resource('dynamodb', region_name=region)

# S3 configuration
bucket = 'monte-carlo-raw-data-william-chang'
stats_key = 'processed_data/portfolio_stats.csv'
returns_key = 'processed_data/portfolio_returns.csv'
job_index = os.getenv('AWS_BATCH_JOB_ARRAY_INDEX', '0')
simulated_results_key = f'processed_data/sim_results_{job_index}.csv'

# DynamoDB configuration
dynamodb_table = 'MonteCarloSimulations'
opt_dynamodb_table = "OptimalPortfolios"

# Simulation parameters
num_simulations = 1000
initial_portfolio_value = 100000
risk_free_rate = 0.01
num_portfolios = 10
num_days = 252
T = 1

def safe_decimal(value, default=0.0, precision=8):
    try:
        if pd.isna(value) or not np.isfinite(value):
            value = default
        d = Decimal(str(value))
        quantizer = Decimal("1." + "0" * precision)
        return d.quantize(quantizer, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError, TypeError) as e:
        return Decimal(str(default)).quantize(Decimal("1." + "0" * precision), rounding=ROUND_HALF_UP)

def read_data_from_s3(bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = pd.read_csv(response['Body'], index_col=0)
        return data
    except Exception as e:
        print(f"Error reading from S3: {str(e)}")
        raise

def write_results_to_s3(bucket, key, df):
    try:
        local_file = f'/tmp/sim_results_{job_index}.csv'
        df.to_csv(local_file)
        s3_client.upload_file(local_file, bucket, key)
        return f"s3://{bucket}/{key}"
    except Exception as e:
        print(f"Error writing to S3: {str(e)}")
        raise
    
def write_metadata_to_dynamodb(table_name, simulation_id, run_date,
                               initial_portfolio_value, num_simulations, returns, volatility,
                               sharpe, prob_loss, VaR_95, weights, num_days, status):
    try:
        table = dynamodb.Table(table_name)

        item = {
            'SimulationID': simulation_id,
            'RunDate': run_date,
            'InitialValue': int(initial_portfolio_value),
            'NumSimulations': int(num_simulations),
            'ExpectedReturns': safe_decimal(returns),
            'Volatility': safe_decimal(volatility),
            'Sharpe': safe_decimal(sharpe),
            'VaR_95': safe_decimal(VaR_95),
            'ProbabilityLoss': safe_decimal(prob_loss),
            'Weights': {k: safe_decimal(v) for k, v in weights.items()},
            'TimeHorizon': int(num_days),
            'Status': status
        }

        table.put_item(Item=item)
    except Exception as e:
        print(f"Error writing to DynamoDB: {str(e)}")
        raise


def main():
    try:
        print("Starting Monte Carlo simulation batch job at " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        run_date = datetime.now().strftime('%Y-%m-%d')

        stats = read_data_from_s3(bucket, stats_key)
        returns = read_data_from_s3(bucket, returns_key)

        mean_returns = stats['MeanReturn_Annual'].values
        cov_matrix = returns.cov() * 252
        num_assets = len(mean_returns)
        asset_names = stats.index.tolist()

        results = {
            'simulation_id': [],
            'run_date': run_date,
            'initial_portfolio_value': initial_portfolio_value,
            'num_simulations': num_simulations,
            'returns': [],
            'volatility': [],
            'sharpe_ratio': [],
            'prob_loss': [],
            'VaR_95': [],
            'weights': [],
        }

        for _ in range(num_portfolios):
            simulation_id = f"sim_{uuid.uuid4()}"
            weights = np.random.dirichlet(np.ones(num_assets), 1)[0]
            weights_dict = {asset: float(w) for asset, w in zip(asset_names, weights)}

            simulated_returns = []
            for _ in range(num_simulations):
                prices = np.ones((T * num_days + 1, num_assets))
                for t in range(1, T * num_days + 1):
                    rand = np.random.normal(0, 1, num_assets)
                    variance = np.diag(cov_matrix.values)
                    drift = (mean_returns - 0.5 * variance) / num_days
                    diffusion = np.dot(np.linalg.cholesky(cov_matrix), rand) * np.sqrt(1 / num_days)
                    prices[t] = prices[t - 1] * np.exp(drift + diffusion)

                final_return = np.dot(prices[-1], weights) - 1.0
                simulated_returns.append(final_return)

            simulated_returns = np.array(simulated_returns)

            expected_returns = simulated_returns.mean()
            volatility = simulated_returns.std()
            sharpe = (expected_returns - risk_free_rate) / volatility
            prob_loss = np.mean(simulated_returns < 0)
            var_95 = np.percentile(simulated_returns, 5)

            results['simulation_id'].append(simulation_id)
            results['returns'].append(expected_returns)
            results['volatility'].append(volatility)
            results['sharpe_ratio'].append(sharpe)
            results['prob_loss'].append(prob_loss)
            results['VaR_95'].append(var_95)
            results['weights'].append(weights_dict)

            write_metadata_to_dynamodb(
                dynamodb_table, simulation_id, run_date, initial_portfolio_value, num_simulations,
                expected_returns, volatility, sharpe, prob_loss, var_95, weights_dict, num_days, "Completed"
            )

        results_df = pd.DataFrame(results)

        write_results_to_s3(bucket, simulated_results_key, results_df)


        print(f"All {num_portfolios} portfolio simulations completed at " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        return {'statusCode': 200, 'body': f"Completed {num_portfolios} simulations"}
    except Exception as e:
        error_msg = f"Error occurred: {str(e)}"
        print(error_msg)
        return {'statusCode': 500, 'body': error_msg}

if __name__ == "__main__":
    main()
