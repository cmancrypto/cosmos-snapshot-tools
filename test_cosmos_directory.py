#!/usr/bin/env python3
"""
Test script for chains.cosmos.directory integration

This script tests fetching chain information from chains.cosmos.directory
and verifying that we can properly access REST endpoints.
"""

import asyncio
import json
import logging
import sys
import urllib.parse
from datetime import datetime

import aiohttp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"test_cosmos_directory_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Test chains
TEST_CHAINS = ["cosmos", "osmosis", "juno"]

async def fetch_chain_info(session, chain_name):
    """Fetch chain information from chains.cosmos.directory."""
    url = f"https://chains.cosmos.directory/{chain_name}"
    logger.info(f"Fetching chain info for {chain_name} from {url}")
    
    try:
        async with session.get(url, timeout=30) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                logger.error(f"Failed to fetch chain info: HTTP {response.status}")
                return None
    except Exception as e:
        logger.error(f"Error fetching chain info: {str(e)}")
        return None

async def test_rest_endpoint(session, endpoint, path="cosmos/base/tendermint/v1beta1/node_info"):
    """Test if a REST endpoint is working."""
    url = f"{endpoint.rstrip('/')}/{path}"
    logger.info(f"Testing REST endpoint: {url}")
    
    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                logger.info(f"Endpoint {endpoint} is working")
                return True, data
            else:
                logger.warning(f"Endpoint {endpoint} returned status {response.status}")
                return False, None
    except Exception as e:
        logger.warning(f"Endpoint {endpoint} failed: {str(e)}")
        return False, None

async def test_validators_query(session, endpoint, pagination_limit=100):
    """Test fetching validators with pagination."""
    path = f"cosmos/staking/v1beta1/validators?pagination.limit={pagination_limit}"
    logger.info(f"Testing validators query: {endpoint}/{path}")
    
    try:
        async with session.get(f"{endpoint.rstrip('/')}/{path}", timeout=15) as response:
            if response.status == 200:
                data = await response.json()
                validators = data.get("validators", [])
                pagination = data.get("pagination", {})
                next_key = pagination.get("next_key")
                
                logger.info(f"Retrieved {len(validators)} validators, next_key: {next_key}")
                
                # Test pagination if there's a next_key
                if next_key:
                    # URL-encode the pagination key
                    encoded_key = urllib.parse.quote(next_key)
                    pagination_path = f"cosmos/staking/v1beta1/validators?pagination.limit={pagination_limit}&pagination.key={encoded_key}"
                    
                    logger.info(f"Testing pagination with key: {next_key}")
                    logger.info(f"Encoded path: {pagination_path}")
                    
                    try:
                        async with session.get(f"{endpoint.rstrip('/')}/{pagination_path}", timeout=15) as page_response:
                            if page_response.status == 200:
                                page_data = await page_response.json()
                                page_validators = page_data.get("validators", [])
                                logger.info(f"Pagination successful, retrieved {len(page_validators)} more validators")
                                return True, len(validators) + len(page_validators)
                            else:
                                logger.warning(f"Pagination failed with status {page_response.status}")
                                return False, len(validators)
                    except Exception as e:
                        logger.warning(f"Pagination request failed: {str(e)}")
                        return False, len(validators)
                
                return True, len(validators)
            else:
                logger.warning(f"Validators query failed with status {response.status}")
                return False, 0
    except Exception as e:
        logger.warning(f"Validators query failed: {str(e)}")
        return False, 0

async def main():
    """Main function to test chains.cosmos.directory integration."""
    results = {}
    
    async with aiohttp.ClientSession() as session:
        for chain_name in TEST_CHAINS:
            chain_result = {"name": chain_name, "working_endpoints": [], "validator_counts": {}}
            
            # Fetch chain info
            chain_info = await fetch_chain_info(session, chain_name)
            if not chain_info:
                logger.error(f"Failed to get info for {chain_name}, skipping...")
                continue
            
            # Extract REST endpoints
            apis = chain_info.get("chain", {}).get("apis", {})
            rest_endpoints = apis.get("rest", [])
            
            if not rest_endpoints:
                logger.warning(f"No REST endpoints found for {chain_name}")
                continue
            
            logger.info(f"Found {len(rest_endpoints)} REST endpoints for {chain_name}")
            
            # Test each endpoint
            for endpoint_info in rest_endpoints:
                endpoint = endpoint_info.get("address")
                provider = endpoint_info.get("provider", "unknown")
                
                if not endpoint:
                    continue
                
                # Test basic endpoint functionality
                success, _ = await test_rest_endpoint(session, endpoint)
                
                if success:
                    # Test validators query
                    validators_success, validator_count = await test_validators_query(session, endpoint)
                    
                    if validators_success:
                        chain_result["working_endpoints"].append({
                            "address": endpoint,
                            "provider": provider,
                            "supports_pagination": validators_success
                        })
                        chain_result["validator_counts"][endpoint] = validator_count
            
            results[chain_name] = chain_result
            logger.info(f"Completed testing {chain_name}, found {len(chain_result['working_endpoints'])} working endpoints")
    
    # Save results
    with open("cosmos_directory_test_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    logger.info("Test complete, results saved to cosmos_directory_test_results.json")
    
    # Print summary
    print("\nTest Results Summary:")
    print("=====================")
    for chain_name, chain_result in results.items():
        working_count = len(chain_result.get("working_endpoints", []))
        total_count = len(chain_info.get("chain", {}).get("apis", {}).get("rest", []))
        print(f"{chain_name}: {working_count}/{total_count} working endpoints")
        
        if working_count > 0:
            for i, endpoint in enumerate(chain_result.get("working_endpoints", [])[:3]):
                print(f"  {i+1}. {endpoint['address']} (Provider: {endpoint['provider']})")
            
            if working_count > 3:
                print(f"  ... and {working_count - 3} more")
    
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
        sys.exit(1) 