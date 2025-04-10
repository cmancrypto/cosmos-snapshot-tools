#!/bin/bash

# Install dependencies if needed
pip install -r requirements.txt

# First, run the test script to check endpoint functionality
echo "Testing chains.cosmos.directory integration..."
python test_cosmos_directory.py

echo "The test script will identify working endpoints and verify pagination functionality."
echo "This helps avoid issues with pagination tokens that some endpoints might not support."
echo ""
echo "Press Enter to continue with the main script or Ctrl+C to cancel..."
read

# Example 1: Query a single chain (cosmoshub) using chains.cosmos.directory
echo "Querying Cosmos Hub with chains.cosmos.directory endpoints..."
python staking_query.py --chains cosmos --output stakers_cosmoshub.json

# Example 2: Query multiple chains with increased concurrency
echo "Querying multiple chains with increased concurrency..."
python staking_query.py --chains cosmos,osmosis,juno --concurrency 3 --output stakers_multiple.json

# Example 3: Query a specific chain with enhanced reliability settings
echo "Querying Osmosis with enhanced reliability settings..."
python staking_query.py --chains osmosis --validator-retries 15 --retries 8 --output stakers_osmosis_reliable.json

# Example 4: See the API endpoints used with their error counts
echo "The output JSON will include API endpoint information with error counts"
echo "You can examine this in the output files"

# Example 5: Filter stakers based on minimum thresholds if needed
echo "Filtering stakers based on minimum thresholds..."
python filter_stakers.py --input stakers_cosmoshub.json --output filtered_cosmoshub.json --min-threshold "cosmos:1000000"

echo "Done! Check the output files for results." 