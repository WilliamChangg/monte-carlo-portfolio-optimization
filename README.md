## Monte Carlo Simulations for Portfolio Optimization

### AWS Services used: IAM, S3, DynamoDB, Lambda, Batch, EC2, ECR, Step Functions, Cloudwatch
### Technical Libraries used: Pandas, Numpy, Matplotlib, YFinance
### High Level Architecture: Leveraged AWS services to preprocess financial data, run large-scale parallel simulations, analyze risk-return profiles, and identify optimal asset allocations—all orchestrated through serverless workflows
1. Fetch and process historical financial data: Created YFinance Layer with EC2 (dockerization) to gather >5 years of data in Lambda script. Used tickers from a variety of sectors, checked covariance with a covariance matrix (Want a low value). For this project, I used META, GM, NVDA, JPM, GAP, GLD, PLTR, SPY. Analyzed and returned annualized mean returns and volatility; saved into s3 bucket for further processing in AWS Batch.
2. Run monte-carlo simulations to model future portfolio outcomes in AWS Batch: Dockerized python script to a container image in Docker, then pushed to ECR to use in AWS Batch. Ran 10 concurrent jobs where each job predicts 100 portfolio price paths with 10,000 simulations per portfolio on EC2 servers for cost optimization.
3. Storage: Stored simulation results in S3 (1M+ time series paths) and metadata (SimID, Final Portfolio Value, Expected Returns, Sharpe Ratio, Volatility, VaR, SD) for analysis in DynamoDB (faster performance, low-latency).
4. Visualization: Visualized results with a risk-return scatter plot and an efficient frontier with matplotlib in AWS Lambda. Stored plots in S3.
5. Automation: Used AWS Step functions to automate and manage end-to-end workflow from data collection to visualization: 1) Trigger AWS lambda functions (fetch_data -> get_statistical_paramters) 2) Start AWS batch jobs (monte_carlo_sim) 3) Trigger lambda func (combine_results -> visualize_results). 4) Included Cloudwatch to track and handle logging and errors.


Total Cost of Project: $3.42 (Mostly EC2 costs for AWS Batch jobs)


