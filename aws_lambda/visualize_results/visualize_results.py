import pandas as pd
import boto3
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import io

# AWS Configuration
region = 'us-east-1'
bucket = 'monte-carlo-raw-data-william-chang'
simulations_key = 'processed_data/sim_results_combined.csv'

# Portfolio parameters
initial_portfolio_value = 100000
num_days = 252

# Initialize AWS clients
s3_client = boto3.client('s3', region_name=region)

def fetch_csv_from_s3(bucket, key):
    """Fetch CSV data from S3"""
    try:
        print(f"Fetching CSV from s3://{bucket}/{key}")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(response['Body'], index_col=0 if 'simulation_id' in pd.read_csv(response['Body'], nrows=0).columns else None)
        print(f"Successfully loaded CSV with {len(df)} rows")
        return df
    except Exception as e:
        print(f"Error fetching CSV from S3: {str(e)}")
        raise

def identify_optimal_portfolios(results_df, initial_portfolio_value, num_days):
    """Identify optimal portfolios based on max Sharpe Ratio and min Volatility"""
    try:
        max_sharpe_idx = results_df['sharpe_ratio'].idxmax()
        min_volatility_idx = results_df['volatility'].idxmin()
        
        max_sharpe_row = results_df.loc[max_sharpe_idx]
        min_vol_row = results_df.loc[min_volatility_idx]
        
        max_sharpe_id = max_sharpe_row['simulation_id']
        min_vol_id = min_vol_row['simulation_id']
        
        expected_max_sharpe_value = initial_portfolio_value * np.exp(max_sharpe_row['returns'])
        expected_min_volatility_value = initial_portfolio_value * np.exp(min_vol_row['returns'])
        
        optimal_df = pd.DataFrame([
            {
                'simulation_id': max_sharpe_id,
                'returns': max_sharpe_row['returns'],
                'volatility': max_sharpe_row['volatility'],
                'sharpe_ratio': max_sharpe_row['sharpe_ratio'],
                'RunType': 'Optimal_Sharpe',
                'ExpectedValue': expected_max_sharpe_value
            },
            {
                'simulation_id': min_vol_id,
                'returns': min_vol_row['returns'],
                'volatility': min_vol_row['volatility'],
                'sharpe_ratio': min_vol_row['sharpe_ratio'],
                'RunType': 'Optimal_MinVol',
                'ExpectedValue': expected_min_volatility_value
            }
        ])
        return optimal_df
    except Exception as e:
        print(f"Error identifying optimal portfolios: {str(e)}")
        raise

def upload_plot_to_s3(fig, bucket, s3_key):
    """Save matplotlib figure to an in-memory buffer and upload to S3"""
    try:
        buffer = io.BytesIO()
        fig.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        s3_client.upload_fileobj(buffer, bucket, s3_key, ExtraArgs={'ContentType': 'image/png'})
        print(f"Uploaded plot to s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload plot: {str(e)}")
        raise

def plot_risk_return_scatter(all_portfolios_df, optimal_df, bucket, s3_key):
    """Plot risk-return scatter and upload to S3"""
    fig, ax = plt.subplots(figsize=(10, 6))
    scatter = ax.scatter(all_portfolios_df['volatility'], all_portfolios_df['returns'],
                         c=all_portfolios_df['sharpe_ratio'], cmap='viridis', alpha=0.5, label='All Portfolios')
    plt.colorbar(scatter, ax=ax, label='Sharpe Ratio')
    if not optimal_df.empty:
        for _, opt in optimal_df.iterrows():
            ax.scatter(opt['volatility'], opt['returns'], s=200, marker='*',
                       label=f"Optimal ({opt['RunType']})")
    ax.set_xlabel('Volatility (Risk)')
    ax.set_ylabel('Expected Return')
    ax.set_title('Risk-Return Scatter of Simulated Portfolios')
    ax.legend()
    ax.grid(True)

    upload_plot_to_s3(fig, bucket, s3_key)
    plt.close(fig)

def plot_efficient_frontier(all_portfolios_df, optimal_df, bucket, s3_key):
    """Plot efficient frontier and upload to S3"""
    df_sorted = all_portfolios_df.sort_values(by='volatility')
    df_sorted['RiskBin'] = pd.qcut(df_sorted['volatility'], q=50, duplicates='drop')
    frontier = df_sorted.loc[df_sorted.groupby('RiskBin')['returns'].idxmax()]
    frontier = frontier.sort_values(by='volatility')
    
    fig, ax = plt.subplots(figsize=(10, 6))
    scatter = ax.scatter(all_portfolios_df['volatility'], all_portfolios_df['returns'],
                         c=all_portfolios_df['sharpe_ratio'], cmap='viridis', alpha=0.5, label='All Portfolios')
    plt.colorbar(scatter, ax=ax, label='Sharpe Ratio')
    ax.plot(frontier['volatility'], frontier['returns'], 'r-', label='Efficient Frontier')
    if not optimal_df.empty:
        for _, opt in optimal_df.iterrows():
            ax.scatter(opt['volatility'], opt['returns'], s=200, marker='*',
                       label=f"Optimal ({opt['RunType']})")
    ax.set_xlabel('Volatility (Risk)')
    ax.set_ylabel('Expected Return')
    ax.set_title('Efficient Frontier from Monte Carlo Simulations')
    ax.legend()
    ax.grid(True)

    upload_plot_to_s3(fig, bucket, s3_key)
    plt.close(fig)

def lambda_handler(event=None, context=None):
    """Main Lambda entry point"""
    try:
        print("Starting visualization process at " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        all_portfolios = fetch_csv_from_s3(bucket, simulations_key)
        optimal_portfolios = identify_optimal_portfolios(all_portfolios, initial_portfolio_value, num_days)

        scatter_plot_key = 'plots/risk_return_scatter.png'
        frontier_plot_key = 'plots/efficient_frontier.png'

        plot_risk_return_scatter(all_portfolios, optimal_portfolios, bucket, scatter_plot_key)
        plot_efficient_frontier(all_portfolios, optimal_portfolios, bucket, frontier_plot_key)

        print("Visualization completed successfully at " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        return {
            'statusCode': 200,
            'body': 'Plots generated and uploaded to S3 successfully.'
        }
    except Exception as e:
        print(f"Visualization failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }