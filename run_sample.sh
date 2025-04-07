#!/bin/bash

# Install dependencies
pip install -r requirements.txt

# Run with default chains
echo "Running with default chains..."
python staking_query.py --output stakers_default.json

# Run with specific chains
echo "Running with specific chains..."
python staking_query.py --chains cosmos,osmosis,juno --output stakers_specific.json

# Example with increased concurrency
echo "Running with increased concurrency..."
python staking_query.py --chains cosmos,osmosis --concurrency 5 --output stakers_concurrent.json

# Example with enhanced retry mechanism for maximum reliability
echo "Running with enhanced retry mechanism..."
python staking_query.py --chains cosmos --validator-retries 15 --retries 8 --output stakers_reliable.json

# Example running without recovery mode (faster but less complete)
echo "Running without recovery mode (faster but less complete)..."
python staking_query.py --chains cosmos --no-recovery --output stakers_no_recovery.json

# Example using a configuration file
echo "Running with configuration file..."
python staking_query.py --config example_chains.json

# Filter examples
echo "Filtering stakers based on minimum thresholds..."
python filter_stakers.py --input stakers_default.json --output filtered_min_threshold.json --min-threshold "cosmos:1000000,osmosis:500000"

# Example running the filter with a configuration file
echo "Running filter with configuration file..."
python filter_stakers.py --config example_filter.json

# Example running the full pipeline with configuration files
echo "Running full pipeline with configuration files..."
python staking_query.py --config example_chains.json
python filter_stakers.py --config example_filter.json 