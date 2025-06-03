## Monte Carlo Simulations for Portfolio Optimization

### AWS Services used: IAM, S3, DynamoDB, Lambda, Batch, EC2, ECR, Step Functions, Cloudwatch
### Technical Libraries used: Pandas, Numpy, Matplotlib, YFinance
### High Level Architecture: Leveraged AWS services to preprocess financial data, run large-scale parallel simulations, analyze risk-return profiles, and identify optimal asset allocations—all orchestrated through serverless workflows
1. Fetch and process historical financial data: Created YFinance Layer with EC2 (dockerization) to gather >5 years of data in Lambda script. Used tickers from a variety of sectors, checked covariance with a covariance matrix (Want a low value). For this project, I used META, GM, NVDA, JPM, GAP, GLD, PLTR, SPY. 
2. Run monte-carlo simulations to model future portfolio outcomes


