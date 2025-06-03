## Monte Carlo Simulations for Portfolio Optimization

### AWS Services used: IAM, S3, DynamoDB, Lambda, Batch, EC2, ECR, Step Functions, Cloudwatch
### High Level Architecture: 
Leveraged AWS services to preprocess financial data, run large-scale parallel simulations, analyze risk-return profiles, and identify optimal asset allocations—all orchestrated through serverless workflows
1. Fetch and process historical financial data: Created YFinance Layer with EC2 (dockerization) to gather >5 years of data in Lambda script. Used tickers from a variety of sectors, checked covariance with a covariance matrix (Want a low value). For this project, I used META, GM, NVDA, JPM, GAP, GLD, PLTR, SPY. Analyzed and returned annualized mean returns and volatility; saved into s3 bucket for further processing in AWS Batch.
2. Run monte-carlo simulations to model future portfolio outcomes in AWS Batch: Dockerized python script to a container image in Docker, then pushed to ECR to use in AWS Batch. Ran 10 parallel processing jobs where each job predicts 100 portfolio price paths with 10,000 simulations per portfolio on EC2 servers for cost optimization.
3. Optimization: Combined .csv files from Batch jobs (10 .csv files) into one then ran script to find optimal max sharpe/min vol portfolios out of pool of 10,000 portfolios inside AWS lambda. Stored results into DynamoDB table.
4. Storage: Stored simulation results in S3 (1M+ time series paths) and metadata (SimID, Final Portfolio Value, Expected Returns, Sharpe Ratio, Volatility, VaR, SD) for analysis in DynamoDB (faster performance, low-latency).
5. Visualization: Visualized results with a risk-return scatter plot and an efficient frontier with matplotlib in AWS Lambda. Stored plots in S3.
6. Automation: Used AWS Step functions to automate and manage end-to-end workflow from data collection to visualization: 1) Trigger AWS lambda functions (fetch_data -> get_statistical_paramters) 2) Start AWS batch jobs (monte_carlo_sim) 3) Trigger lambda func (combine_results -> visualize_results). 4) Included Cloudwatch to track and handle logging and errors.

### Results (Initial Portfolio Value: $100,000):
**Optimal Max Sharpe Portfolio** (After 252 Trading Days): Returns: $137696.19, Sharpe: 1.202, Volatility: 0.258

**Weights:** {GAP: 0.0357, GLD: 0.499, GM: 0.0087, JPM: 0.235, META: 0.00466, NVDA: 0.0732, PLTR: 0.0695, SPY: 0.0741}

**Optimal Min Sharpe Portfolio** (After 252 Trading Days): Returns: $121964.28, Sharpe: 0.977, Volatility: 0.193

**Weights:** {GAP: 0.123, GLD: 0.679, GM: 0.0564, JPM: 0.000379, META: 0.00508, NVDA: 0.0332, PLTR: 0.0202, SPY: 0.0827}

<img width="804" alt="image" src="https://github.com/user-attachments/assets/03fe0843-638c-4b00-966b-7c4677bf4b76" />

Total Cost of Project: $3.42 (Mostly EC2 costs for AWS Batch jobs)

Resources Used: 

DOI: 10.54254/2754-1169/50/20230568

https://www.investopedia.com/terms/m/montecarlosimulation.asp

https://medium.com/@beingamanforever/portfolio-optimisation-using-monte-carlo-simulation-25d88003782e

[AWS Documentations](https://docs.aws.amazon.com/)

