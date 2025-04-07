#!/bin/bash

# Install dependencies
pip install -r requirements.txt

# Run with default chains
echo "Running with default chains..."
python staking_query.py --output delegator_data_default.csv

# Run with specific chains
echo "Running with specific chains..."
python staking_query.py --chains cosmos,osmosis,juno --output delegator_data_specific.csv

# Run with configuration file
echo "Running with configuration file..."
python staking_query.py --config example_config.json --output delegator_data_config.csv

# Example with increased concurrency
echo "Running with increased concurrency..."
python staking_query.py --config example_config.json --concurrency 5 --output delegator_data_concurrent.csv

# Example with enhanced retry mechanism for maximum reliability
echo "Running with enhanced retry mechanism..."
python staking_query.py --chains cosmos --validator-retries 15 --retries 8 --output delegator_data_reliable.csv

# Example running without recovery mode (faster but less complete)
echo "Running without recovery mode (faster but less complete)..."
python staking_query.py --chains cosmos --no-recovery --output delegator_data_no_recovery.csv 