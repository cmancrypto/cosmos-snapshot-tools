#!/usr/bin/env python3
"""
Cosmos Staking Query Tool

This script queries Cosmos chains for delegator data and calculates total 
staked amounts for each delegator.
"""

import argparse
import asyncio
import json
import logging
from datetime import datetime
import time
import random
import sys
import re
import urllib.parse
from typing import Dict, List, Set, Tuple, Any, Optional
from collections import defaultdict

import aiohttp
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"staking_query_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Default chains to query if none specified
DEFAULT_CHAINS = ["cosmos", "osmosis", "juno"]

class RestEndpoint:
    """Class to track a REST endpoint and its error count."""
    
    def __init__(self, address: str, provider: str = ""):
        self.address = address
        self.provider = provider
        self.error_count = 0
        
    def increment_error(self):
        """Increment the error count for this endpoint."""
        self.error_count += 1
        
    def __str__(self) -> str:
        return f"{self.address} (Provider: {self.provider}, Errors: {self.error_count})"


class ChainConfig:
    """Stores configuration data for a Cosmos chain."""
    
    def __init__(self, name: str):
        self.name = name
        self.pretty_name = ""
        self.chain_id = ""
        self.bech32_prefix = ""
        self.rest_endpoints = []  # List of RestEndpoint objects
        self.is_configured = False
    
    def add_rest_endpoint(self, address: str, provider: str = ""):
        """Add a REST endpoint to this chain's configuration."""
        self.rest_endpoints.append(RestEndpoint(address, provider))
    
    def get_rest_endpoint(self, offset: int = 0) -> Optional[str]:
        """
        Get the REST endpoint with the lowest error count, plus an offset.
        
        Args:
            offset: Skip this many endpoints (sorted by error count)
            
        Returns:
            The endpoint address or None if no endpoints available
        """
        if not self.rest_endpoints:
            return None
            
        # Sort endpoints by error count
        sorted_endpoints = sorted(self.rest_endpoints, key=lambda e: e.error_count)
        
        # Get the endpoint at the specified offset (or the last one if offset too large)
        endpoint_index = min(offset, len(sorted_endpoints) - 1)
        endpoint = sorted_endpoints[endpoint_index]
        
        logger.debug(f"Using endpoint {endpoint} for {self.name} (offset {offset})")
        return endpoint.address
    
    def record_endpoint_error(self, endpoint_address: str):
        """Record an error for the specified endpoint."""
        for endpoint in self.rest_endpoints:
            if endpoint.address == endpoint_address:
                endpoint.increment_error()
                logger.warning(f"Recorded error for endpoint {endpoint_address} (now has {endpoint.error_count} errors)")
                break
    
    def get_endpoints_report(self) -> List[Dict]:
        """
        Generate a detailed report of all REST endpoints with their error counts.
        
        Returns:
            List of dictionaries with endpoint data
        """
        # Sort endpoints by error count (lowest first)
        sorted_endpoints = sorted(self.rest_endpoints, key=lambda e: e.error_count)
        
        return [
            {
                "address": endpoint.address,
                "provider": endpoint.provider,
                "error_count": endpoint.error_count
            }
            for endpoint in sorted_endpoints
        ]
    
    def __str__(self) -> str:
        endpoints_str = ", ".join([e.address for e in self.rest_endpoints[:3]])
        if len(self.rest_endpoints) > 3:
            endpoints_str += f" and {len(self.rest_endpoints) - 3} more"
        return f"Chain: {self.pretty_name} ({self.name}), ID: {self.chain_id}, Prefix: {self.bech32_prefix}, Endpoints: {endpoints_str}"


class CosmosStakingQueryTool:
    """
    Tool for querying delegator data from Cosmos chains.
    
    This class handles:
    - Fetching chain configuration
    - Querying validators
    - Collecting delegator data
    - Aggregating delegations by address
    """
    
    def __init__(self, chains: List[str], output_file: str, max_retries: int = 5, 
                 concurrency_limit: int = 3, validator_retries: int = 10, enable_recovery: bool = True):
        self.chain_configs = {
            name: ChainConfig(name) 
            for name in chains
        }
        self.output_file = output_file
        self.max_retries = max_retries
        self.validator_retries = validator_retries
        self.concurrency_limit = concurrency_limit
        self.enable_recovery = enable_recovery
        self.all_stakers_data = {}  # Dictionary to store all staker data
        self.failed_validators = {}  # To track validators that fail even after retries
        
        # Track pagination errors to adjust pagination size adaptively
        self.pagination_error_count = {}  # Chain name -> error count
        self.chain_pagination_sizes = {}  # Chain name -> current pagination size
        
        # Track which request is at which concurrency level
        self.request_counter = defaultdict(int)
    
    async def run(self):
        """Main execution function to process all chains."""
        logger.info(f"Starting staking query for {len(self.chain_configs)} chains")
        
        # Fetch chain configurations
        async with aiohttp.ClientSession() as session:
            await self.fetch_all_chain_configs(session)
            
            # Create tasks for each chain to process in parallel
            tasks = []
            for chain_name, chain_config in self.chain_configs.items():
                if chain_config.is_configured:
                    tasks.append(self.process_chain(session, chain_config))
                else:
                    logger.warning(f"Skipping {chain_name} due to missing configuration")
            
            # Process chains with concurrency limit
            results = []
            for i in range(0, len(tasks), self.concurrency_limit):
                batch = tasks[i:i+self.concurrency_limit]
                batch_results = await asyncio.gather(*batch)
                results.extend(batch_results)
            
        # Post-process and save results
        self.finalize_results()
        return self.all_stakers_data
    
    async def fetch_all_chain_configs(self, session: aiohttp.ClientSession):
        """Fetch configuration for all chains."""
        logger.info("Fetching chain configurations")
        tasks = [self.fetch_chain_config(session, chain_config) 
                for chain_config in self.chain_configs.values()]
        await asyncio.gather(*tasks)
    
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=60))
    async def fetch_chain_config(self, session: aiohttp.ClientSession, chain_config: ChainConfig):
        """Fetch configuration for a single chain."""
        url = f"https://chains.cosmos.directory/{chain_config.name}"
        logger.info(f"Fetching config for {chain_config.name} from {url}")
        
        try:
            async with session.get(url, timeout=30) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch config for {chain_config.name}: HTTP {response.status}")
                    # Try with normalized name (sometimes directory uses different naming)
                    alt_name = chain_config.name.replace('hub', '')
                    alt_url = f"https://chains.cosmos.directory/{alt_name}"
                    
                    if alt_name != chain_config.name:
                        logger.info(f"Trying alternative name: {alt_name}")
                        try:
                            async with session.get(alt_url, timeout=30) as alt_response:
                                if alt_response.status == 200:
                                    data = await alt_response.json()
                                    logger.info(f"Successfully fetched config using alternative name {alt_name}")
                                else:
                                    logger.error(f"Failed with alternative name too: HTTP {alt_response.status}")
                                    return
                        except Exception as e:
                            logger.error(f"Error with alternative name: {str(e)}")
                            return
                    else:
                        return
                else:
                    data = await response.json()
                
                # Extract chain data from response
                if "chain" not in data:
                    logger.error(f"Invalid response format from {url}: 'chain' field missing")
                    return
                    
                chain_data = data.get("chain", {})
                
                chain_config.pretty_name = chain_data.get("pretty_name", chain_config.name)
                chain_config.chain_id = chain_data.get("chain_id", "")
                chain_config.bech32_prefix = chain_data.get("bech32_prefix", "")
                
                # Get REST endpoints
                apis = chain_data.get("apis", {})
                rest_endpoints = apis.get("rest", [])
                
                if rest_endpoints:
                    # Add all REST endpoints
                    for endpoint in rest_endpoints:
                        address = endpoint.get("address", "")
                        provider = endpoint.get("provider", "")
                        if address:
                            chain_config.add_rest_endpoint(address, provider)
                            logger.debug(f"Added REST endpoint {address} for {chain_config.name} (provider: {provider})")
                
                # Fallback to a default endpoint if none found
                if not chain_config.rest_endpoints:
                    logger.warning(f"No REST endpoints found for {chain_config.name}, adding default")
                    
                    # Try multiple possible default endpoints
                    default_endpoints = [
                        f"https://rest.cosmos.directory/{chain_config.name}",
                        f"https://lcd-{chain_config.name}.keplr.app",
                        f"https://api.{chain_config.name}.zone"
                    ]
                    
                    for endpoint in default_endpoints:
                        chain_config.add_rest_endpoint(endpoint, "default fallback")
                
                chain_config.is_configured = all([
                    chain_config.chain_id,
                    chain_config.bech32_prefix,
                    len(chain_config.rest_endpoints) > 0
                ])
                
                if chain_config.is_configured:
                    logger.info(f"Successfully configured {chain_config.name}: {chain_config}")
                else:
                    logger.warning(f"Incomplete configuration for {chain_config.name}: missing " + 
                                  ", ".join(["chain_id"] if not chain_config.chain_id else []) +
                                  ", ".join(["bech32_prefix"] if not chain_config.bech32_prefix else []) +
                                  ", ".join(["rest_endpoints"] if not chain_config.rest_endpoints else []))
        
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching config for {chain_config.name}")
            raise
        except Exception as e:
            logger.error(f"Error fetching config for {chain_config.name}: {str(e)}")
            raise
    
    async def process_chain(self, session: aiohttp.ClientSession, chain_config: ChainConfig) -> Dict:
        """Process a single chain to get all delegator data."""
        logger.info(f"Processing chain: {chain_config.name}")
        chain_data = {
            "chain_id": chain_config.chain_id,
            "name": chain_config.name,
            "pretty_name": chain_config.pretty_name,
            "bech32_prefix": chain_config.bech32_prefix,
            "stakers": {}
        }
        
        try:
            # Get all validators
            validators = await self.get_all_validators(session, chain_config)
            logger.info(f"Found {len(validators)} validators for {chain_config.name}")
            
            # Process validators in batches to not overwhelm the endpoints
            delegator_data_list = []
            failed_validators = []
            
            # Smaller batch size to reduce rate limiting
            batch_size = 5 
            
            with tqdm(total=len(validators), desc=f"Processing {chain_config.name} validators") as pbar:
                for i in range(0, len(validators), batch_size):
                    validator_batch = validators[i:i+batch_size]
                    
                    # Create tasks for each validator's delegators
                    tasks = [self.get_validator_delegations(session, chain_config, validator) 
                            for validator in validator_batch]
                    
                    # Run tasks concurrently and catch any that fail
                    batch_results = []
                    for task, validator in zip(asyncio.as_completed(tasks), validator_batch):
                        try:
                            result = await task
                            batch_results.append(result)
                        except Exception as e:
                            logger.error(f"Failed to get delegations after retries for validator {validator} on {chain_config.name}: {str(e)}")
                            failed_validators.append(validator)
                    
                    # Flatten results and add to list
                    for delegator_list in batch_results:
                        # Add chain name to each delegation record
                        for delegation in delegator_list:
                            delegation["chain"] = chain_config.name
                        delegator_data_list.extend(delegator_list)
                    
                    pbar.update(len(validator_batch))
                    
                    # Add delay between batches to avoid rate limiting
                    logger.info(f"Completed batch. Waiting before next batch...")
                    await asyncio.sleep(random.uniform(2.0, 5.0))
            
            # Record any failed validators
            if failed_validators:
                self.failed_validators[chain_config.name] = failed_validators
                logger.warning(f"Failed to process {len(failed_validators)} validators for {chain_config.name} after multiple retries")
            
            # Try to recover failed validators with extended backoff if enabled
            if failed_validators and self.enable_recovery:
                logger.info(f"Attempting recovery for {len(failed_validators)} failed validators with extended backoff")
                recovered_data = await self.recover_failed_validators(session, chain_config, failed_validators)
                # Add chain name to each recovered delegation record
                for delegation in recovered_data:
                    delegation["chain"] = chain_config.name
                delegator_data_list.extend(recovered_data)
            elif failed_validators:
                logger.warning(f"Recovery mode disabled - skipping recovery attempts for {len(failed_validators)} validators")
            
            # Create stakers data for this chain
            if delegator_data_list:
                # Convert to DataFrame for easier aggregation
                df = pd.DataFrame(delegator_data_list)
                
                # Aggregate delegations by address
                stakers_df = df.groupby("address").agg({
                    "staked_amount": "sum",
                    "denom": "first"
                }).reset_index()
                
                # Convert to dictionary format for JSON output
                stakers = {}
                for _, row in stakers_df.iterrows():
                    stakers[row["address"]] = {
                        "amount": int(row["staked_amount"]),
                        "denom": row["denom"]
                    }
                
                chain_data["stakers"] = stakers
                chain_data["total_stakers"] = len(stakers)
                
                # Sum the total staked amount
                chain_data["total_staked"] = int(stakers_df["staked_amount"].sum())
                
                # Add the denom if we have stakers
                if len(stakers_df) > 0:
                    chain_data["denom"] = stakers_df.iloc[0]["denom"]
                
                # Add API endpoint info including error counts
                chain_data["api_endpoints"] = chain_config.get_endpoints_report()
                
                logger.info(f"Processed {len(stakers)} stakers for {chain_config.name}")
            else:
                logger.warning(f"No staker data found for {chain_config.name}")
            
            return chain_data
                
        except Exception as e:
            logger.error(f"Error processing chain {chain_config.name}: {str(e)}")
            # Return a minimal chain data structure with error info
            chain_data["error"] = str(e)
            return chain_data
            
    async def recover_failed_validators(self, session: aiohttp.ClientSession, 
                                      chain_config: ChainConfig, 
                                      validators: List[str]) -> List[Dict]:
        """
        Attempt to recover data for validators that failed during regular processing.
        
        This uses a more aggressive approach with longer delays and smaller page sizes.
        """
        logger.info(f"Starting recovery process for {len(validators)} validators on {chain_config.name}")
        all_recovered = []
        
        # Process one validator at a time for recovery
        for i, validator in enumerate(validators):
            logger.info(f"Recovery attempt {i+1}/{len(validators)} for validator {validator}")
            
            try:
                # Use a dedicated recovery method with more aggressive retry logic
                recovered_delegations = await self.attempt_validator_recovery(
                    session, chain_config, validator, 
                    max_attempts=3, base_delay=10, pagination_delay_range=(2, 5)
                )
                
                if recovered_delegations:
                    logger.info(f"Recovered {len(recovered_delegations)} delegations for validator {validator}")
                    all_recovered.extend(recovered_delegations)
                else:
                    logger.warning(f"No delegations recovered for validator {validator}")
                
                # Add a delay between validators to avoid rate limiting
                await asyncio.sleep(random.uniform(5, 10))
                
            except Exception as e:
                logger.error(f"Error during recovery for validator {validator}: {str(e)}")
                # Continue with next validator
        
        logger.info(f"Recovery complete. Recovered data for {len(all_recovered)} delegations across {len(validators)} validators")
        return all_recovered
    
    @retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=2, min=5, max=300))
    async def get_validator_delegations(self, session: aiohttp.ClientSession, 
                                       chain_config: ChainConfig, validator_addr: str) -> List[Dict]:
        """
        Get all delegations for a validator.
        
        Args:
            session: The aiohttp session to use
            chain_config: The chain configuration
            validator_addr: The validator address
            
        Returns:
            List of delegations with amounts
        """
        delegations = []
        next_key = None
        page_count = 0
        failures = 0
        request_id = f"delegations_{validator_addr[-8:]}_{int(time.time())}"
        pagination_size = self.get_pagination_size_for_chain(chain_config.name)
        errors_by_endpoint = {}  # Track which endpoints fail with which pagination keys
        
        logger.info(f"Getting delegations for validator {validator_addr} on {chain_config.name}")
        
        try:
            while True:
                page_count += 1
                
                # Create pagination params - with proper encoding
                if next_key:
                    # URL-encode the pagination key
                    encoded_key = urllib.parse.quote(next_key)
                    pagination_params = f"pagination.limit={pagination_size}&pagination.key={encoded_key}"
                else:
                    pagination_params = f"pagination.limit={pagination_size}"
                
                endpoint_path = f"cosmos/staking/v1beta1/validators/{validator_addr}/delegations?{pagination_params}"
                
                # Try multiple endpoints for this pagination request
                success = False
                last_error = None
                attempted_endpoints = []
                
                # Try up to 3 different endpoints
                for attempt in range(min(3, len(chain_config.rest_endpoints))):
                    try:
                        # Get an endpoint that hasn't failed with this pagination key
                        offset = attempt
                        while offset < len(chain_config.rest_endpoints):
                            endpoint = chain_config.get_rest_endpoint(offset)
                            # Skip endpoints that have already failed with this key
                            if next_key and endpoint in errors_by_endpoint.get(next_key, []):
                                offset += 1
                                continue
                            break
                            
                        if offset >= len(chain_config.rest_endpoints):
                            # We've exhausted all endpoints for this pagination key
                            break
                            
                        attempted_endpoints.append(endpoint)
                        
                        # Custom request to handle pagination issues
                        url = f"{endpoint.rstrip('/')}/{endpoint_path}"
                        logger.debug(f"Requesting delegations (page {page_count}) from {url}")
                        
                        async with session.get(url, timeout=30) as response:
                            if response.status == 200:
                                result = await response.json()
                                
                                # Extract delegations from response
                                page_delegations = result.get("delegation_responses", [])
                                for delegation in page_delegations:
                                    delegator_address = delegation.get("delegation", {}).get("delegator_address", "")
                                    balance = delegation.get("balance", {})
                                    
                                    if delegator_address and balance:
                                        amount = int(balance.get("amount", "0"))
                                        denom = balance.get("denom", "")
                                        
                                        delegations.append({
                                            "address": delegator_address,
                                            "staked_amount": amount,
                                            "denom": denom,
                                            "validator": validator_addr
                                        })
                                
                                # Check if there are more pages
                                pagination = result.get("pagination", {})
                                next_key = pagination.get("next_key")
                                
                                success = True
                                break  # We got data successfully
                            else:
                                # Record endpoint as failing with this pagination key
                                if next_key:
                                    if next_key not in errors_by_endpoint:
                                        errors_by_endpoint[next_key] = []
                                    errors_by_endpoint[next_key].append(endpoint)
                                
                                chain_config.record_endpoint_error(endpoint)
                                error_text = await response.text()
                                last_error = f"HTTP error {response.status} from {url}: {error_text}"
                                logger.warning(f"Endpoint {endpoint} failed with status {response.status} for delegations page {page_count}")
                                
                    except asyncio.TimeoutError:
                        chain_config.record_endpoint_error(endpoint)
                        last_error = f"Timeout error querying {url}"
                        logger.warning(f"Timeout error for endpoint {endpoint}")
                    except Exception as e:
                        chain_config.record_endpoint_error(endpoint)
                        last_error = f"Error querying {url}: {str(e)}"
                        logger.warning(f"Error for endpoint {endpoint}: {str(e)}")
                
                if not success:
                    # All endpoints failed for this pagination request
                    failures += 1
                    if failures >= 3:  # Try a few times before giving up
                        logger.error(f"Persistent errors getting delegations for validator {validator_addr}: {last_error}")
                        self.record_pagination_error(chain_config.name)
                        if page_count == 1:
                            # If we can't even get the first page, that's a fatal error
                            raise Exception(f"Failed to get any delegations for validator {validator_addr}")
                        else:
                            # If we already have some delegations, we'll continue with what we have
                            logger.warning(f"Stopping pagination after getting {len(delegations)} delegations")
                            break
                    
                    # Wait and retry with a different set of endpoints
                    logger.warning(f"All endpoints failed for page {page_count}, waiting before retry")
                    await asyncio.sleep(random.uniform(2.0, 5.0))
                    continue
                
                # Reset failures counter on success
                failures = 0
                
                # If no more pages, we're done
                if not next_key:
                    break
                    
                # Add a small delay between pagination requests to avoid rate limiting
                await asyncio.sleep(random.uniform(0.5, 2.0))
            
            logger.info(f"Retrieved {len(delegations)} delegations for validator {validator_addr} in {page_count} pages")
            return delegations
            
        except Exception as e:
            logger.error(f"Error fetching delegations for validator {validator_addr} on {chain_config.name}: {str(e)}")
            raise
    
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=60))
    async def get_all_validators(self, session: aiohttp.ClientSession, chain_config: ChainConfig) -> List[str]:
        """
        Get all validator addresses for a chain.
        
        Args:
            session: The aiohttp session to use
            chain_config: The chain configuration
            
        Returns:
            List of validator addresses
        """
        validators = []
        next_key = None
        page_count = 0
        request_id = f"validators_{chain_config.name}_{int(time.time())}"
        errors_by_endpoint = {}  # Track which endpoints fail with which pagination keys
        
        try:
            while True:
                page_count += 1
                
                # Create pagination params - important to properly encode the pagination key
                if next_key:
                    # URL-encode the pagination key to ensure special characters are handled properly
                    encoded_key = urllib.parse.quote(next_key)
                    pagination_params = f"pagination.limit=100&pagination.key={encoded_key}"
                else:
                    pagination_params = "pagination.limit=100"
                
                endpoint_path = f"cosmos/staking/v1beta1/validators?{pagination_params}"
                
                # Try multiple endpoints until one works
                success = False
                last_error = None
                attempted_endpoints = []
                
                # Try up to 3 different endpoints for this pagination request
                for attempt in range(min(3, len(chain_config.rest_endpoints))):
                    try:
                        # Get an endpoint that hasn't failed with this pagination key before
                        offset = attempt
                        while offset < len(chain_config.rest_endpoints):
                            endpoint = chain_config.get_rest_endpoint(offset)
                            # Skip endpoints that have already failed with this pagination key
                            if next_key and endpoint in errors_by_endpoint.get(next_key, []):
                                offset += 1
                                continue
                            break
                        
                        if offset >= len(chain_config.rest_endpoints):
                            # We've exhausted all endpoints for this pagination key
                            break
                            
                        attempted_endpoints.append(endpoint)
                        
                        # Custom request instead of using make_request to handle pagination issues
                        url = f"{endpoint.rstrip('/')}/{endpoint_path}"
                        logger.debug(f"Requesting validators (page {page_count}) from {url}")
                        
                        async with session.get(url) as response:
                            if response.status == 200:
                                result = await response.json()
                                
                                # Extract validators from response
                                page_validators = result.get("validators", [])
                                for validator in page_validators:
                                    validator_address = validator.get("operator_address", "")
                                    if validator_address:
                                        validators.append(validator_address)
                                
                                # Check if there are more pages
                                pagination = result.get("pagination", {})
                                next_key = pagination.get("next_key")
                                
                                success = True
                                break  # We successfully got data from this endpoint
                            else:
                                # Record the endpoint as failing with this pagination key
                                if next_key:
                                    if next_key not in errors_by_endpoint:
                                        errors_by_endpoint[next_key] = []
                                    errors_by_endpoint[next_key].append(endpoint)
                                
                                chain_config.record_endpoint_error(endpoint)
                                error_text = await response.text()
                                last_error = f"HTTP error {response.status} from {url}: {error_text}"
                                logger.warning(f"Endpoint {endpoint} failed with status {response.status} for validators page {page_count}")
                    
                    except Exception as e:
                        chain_config.record_endpoint_error(endpoint)
                        last_error = f"Error querying {url}: {str(e)}"
                        logger.warning(f"Endpoint {endpoint} failed with error: {str(e)}")
                
                if not success:
                    # All endpoints failed for this pagination request
                    if page_count == 1:
                        # If we can't even get the first page, that's a fatal error
                        logger.error(f"All endpoints failed to get validators page {page_count}. Last error: {last_error}")
                        raise Exception(f"Failed to get validators from any endpoint. Tried: {', '.join(attempted_endpoints)}")
                    else:
                        # If we've already got some validators, we can continue with what we have
                        logger.warning(f"All endpoints failed for pagination key '{next_key}'. Stopping pagination.")
                        break
                
                # Exit loop if we've reached the end of pagination
                if not next_key:
                    break
                    
                # Add a small delay between pagination requests to avoid rate limiting
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
            logger.info(f"Retrieved {len(validators)} validators for {chain_config.name} in {page_count} pages")
            return validators
            
        except Exception as e:
            logger.error(f"Error fetching validators for {chain_config.name}: {str(e)}")
            raise
    
    def finalize_results(self):
        """Process and save the final results to a JSON file."""
        try:
            # Calculate total stakers count across all chains
            total_stakers = 0
            total_validators_failed = 0
            
            # Add metadata to each chain's data
            for chain_name, chain_data in self.all_stakers_data.items():
                stakers = chain_data.get("stakers", {})
                
                # Add statistics
                chain_data["total_stakers"] = len(stakers)
                total_stakers += len(stakers)
                
                # Calculate total staked amount for this chain
                total_staked = sum(staker_data.get("amount", 0) for staker_data in stakers.values())
                chain_data["total_staked"] = total_staked
                
                # Add common denom info if available
                if stakers:
                    # Get the denom from the first staker (they should all be the same)
                    first_staker = next(iter(stakers.values()))
                    chain_data["denom"] = first_staker.get("denom", "")
                
                # Add API endpoint info including error counts
                chain_config = self.chain_configs.get(chain_name)
                if chain_config:
                    chain_data["api_endpoints"] = chain_config.get_endpoints_report()
            
            # Count total failed validators
            for failed_list in self.failed_validators.values():
                total_validators_failed += len(failed_list)
            
            # Create the final output structure
            result = {
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "chains_processed": len(self.all_stakers_data),
                    "total_stakers": total_stakers,
                    "total_validators_failed": total_validators_failed
                },
                "chains": self.all_stakers_data
            }
            
            # Save to file
            with open(self.output_file, 'w') as f:
                json.dump(result, f, indent=2)
                
            logger.info(f"Results saved to {self.output_file}")
            logger.info(f"Processed {len(self.all_stakers_data)} chains with a total of {total_stakers} unique stakers")
            
            if total_validators_failed > 0:
                logger.warning(f"Failed to process {total_validators_failed} validators across all chains")
                
                # Log detailed info about failed validators by chain
                for chain_name, failed_list in self.failed_validators.items():
                    logger.warning(f"Chain {chain_name}: {len(failed_list)} failed validators")
            
            # Save report of failed validators
            if self.failed_validators:
                failed_report_path = f"failed_validators_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(failed_report_path, 'w') as f:
                    json.dump(self.failed_validators, f, indent=2)
                logger.warning(f"Some validators failed all retry attempts. See {failed_report_path} for details.")
            
        except Exception as e:
            logger.error(f"Error finalizing results: {str(e)}")
    
    def get_pagination_size_for_chain(self, chain_name: str, request_type: str = "delegations") -> int:
        """
        Get the appropriate pagination size for a chain based on its error history.
        Adaptively reduces pagination size if too many errors are encountered.
        
        Args:
            chain_name: The name of the chain
            request_type: Type of request ("delegations" or "recovery")
            
        Returns:
            The pagination size to use
        """
        # If we haven't set a pagination size for this chain yet, initialize it
        if chain_name not in self.chain_pagination_sizes:
            # Start with maximum size for normal delegations
            if request_type == "delegations":
                self.chain_pagination_sizes[chain_name] = 500
            # Use a smaller initial size for recovery requests
            elif request_type == "recovery":
                self.chain_pagination_sizes[chain_name] = 200
            else:
                # Default fallback
                self.chain_pagination_sizes[chain_name] = 150
        
        # Get the current error count for this chain
        error_count = self.pagination_error_count.get(chain_name, 0)
        
        # Adjust pagination size based on error count
        current_size = self.chain_pagination_sizes[chain_name]
        
        # If we have errors, progressively reduce the pagination size
        if error_count > 0:
            if error_count == 1:
                # After first error, reduce to 250
                new_size = min(current_size, 250)
            elif error_count == 2:
                # After second error, reduce to 150
                new_size = min(current_size, 150)
            elif error_count >= 3:
                # After third error and beyond, use conservative 100
                new_size = 100
            
            # Only log if we're actually reducing the size
            if new_size < current_size:
                logger.info(f"Reducing pagination size for {chain_name} from {current_size} to {new_size} due to {error_count} errors")
                self.chain_pagination_sizes[chain_name] = new_size
                
        return self.chain_pagination_sizes[chain_name]
    
    def record_pagination_error(self, chain_name: str):
        """Record that a chain had a pagination-related error."""
        if chain_name not in self.pagination_error_count:
            self.pagination_error_count[chain_name] = 0
        
        self.pagination_error_count[chain_name] += 1
        logger.warning(f"Recorded pagination error for {chain_name} (total: {self.pagination_error_count[chain_name]})")
    
    async def attempt_validator_recovery(self, session: aiohttp.ClientSession, 
                                      chain_config: ChainConfig, validator: str,
                                      max_attempts: int = 3, base_delay: int = 10,
                                      pagination_delay_range: tuple = (2, 5)) -> List[Dict]:
        """
        Make aggressive attempts to recover data for a validator that failed initial processing.
        
        Uses longer delays and more careful pagination handling to maximize chances of success.
        """
        delegations = []
        next_key = None
        attempt = 0
        request_id = f"recovery_{validator[-8:]}_{int(time.time())}"
        errors_by_endpoint = {}  # Track which endpoints fail with which pagination keys
        
        while attempt < max_attempts:
            attempt += 1
            logger.info(f"Recovery attempt {attempt}/{max_attempts} for validator {validator} on {chain_config.name}")
            
            try:
                # Exponential backoff between attempts
                if attempt > 1:
                    delay = base_delay * (2 ** (attempt - 1))
                    logger.info(f"Waiting {delay}s before retry...")
                    await asyncio.sleep(delay)
                
                # Use a very small pagination size for recovery attempts
                pagination_size = min(20, self.get_pagination_size_for_chain(chain_config.name, "recovery"))
                page_count = 0
                
                while True:
                    page_count += 1
                    # Create pagination params with proper encoding
                    if next_key:
                        encoded_key = urllib.parse.quote(next_key)
                        pagination_params = f"pagination.limit={pagination_size}&pagination.key={encoded_key}"
                    else:
                        pagination_params = f"pagination.limit={pagination_size}"
                    
                    endpoint_path = f"cosmos/staking/v1beta1/validators/{validator}/delegations?{pagination_params}"
                    
                    # Try multiple endpoints for this pagination request
                    success = False
                    last_error = None
                    attempted_endpoints = []
                    
                    # Try up to 3 different endpoints
                    for endpoint_attempt in range(min(3, len(chain_config.rest_endpoints))):
                        try:
                            # Get an endpoint that hasn't failed with this pagination key
                            offset = endpoint_attempt
                            while offset < len(chain_config.rest_endpoints):
                                endpoint = chain_config.get_rest_endpoint(offset)
                                # Skip endpoints that have already failed with this key
                                if next_key and endpoint in errors_by_endpoint.get(next_key, []):
                                    offset += 1
                                    continue
                                break
                                
                            if offset >= len(chain_config.rest_endpoints):
                                # We've exhausted all endpoints for this pagination key
                                break
                                
                            attempted_endpoints.append(endpoint)
                            
                            # Custom request with longer timeout for recovery
                            url = f"{endpoint.rstrip('/')}/{endpoint_path}"
                            logger.debug(f"Recovery request for delegations (page {page_count}) from {url}")
                            
                            async with session.get(url, timeout=60) as response:
                                if response.status == 200:
                                    result = await response.json()
                                    
                                    # Extract delegations from response
                                    page_delegations = result.get("delegation_responses", [])
                                    for delegation in page_delegations:
                                        delegator_address = delegation.get("delegation", {}).get("delegator_address", "")
                                        balance = delegation.get("balance", {})
                                        
                                        if delegator_address and balance:
                                            amount = int(balance.get("amount", "0"))
                                            denom = balance.get("denom", "")
                                            
                                            delegations.append({
                                                "address": delegator_address,
                                                "staked_amount": amount,
                                                "denom": denom,
                                                "validator": validator
                                            })
                                    
                                    # Check if there are more pages
                                    pagination = result.get("pagination", {})
                                    next_key = pagination.get("next_key")
                                    
                                    success = True
                                    break  # We got data successfully
                                else:
                                    # Record endpoint as failing with this pagination key
                                    if next_key:
                                        if next_key not in errors_by_endpoint:
                                            errors_by_endpoint[next_key] = []
                                        errors_by_endpoint[next_key].append(endpoint)
                                    
                                    chain_config.record_endpoint_error(endpoint)
                                    error_text = await response.text()
                                    last_error = f"HTTP error {response.status} from {url}: {error_text}"
                                    logger.warning(f"Recovery endpoint {endpoint} failed with status {response.status}")
                                    
                        except Exception as e:
                            chain_config.record_endpoint_error(endpoint)
                            last_error = f"Error in recovery for {validator}: {str(e)}"
                            logger.warning(f"Recovery error for endpoint {endpoint}: {str(e)}")
                    
                    if not success:
                        # All endpoints failed for this pagination request during recovery
                        logger.warning(f"All recovery endpoints failed for page {page_count}. Last error: {last_error}")
                        # If we have some data, return it, otherwise continue to next attempt
                        if delegations:
                            logger.info(f"Partial recovery successful, got {len(delegations)} delegations before failure")
                            return delegations
                        else:
                            break  # Try again with next attempt
                    
                    # If no more pages, we're done with this attempt
                    if not next_key:
                        logger.info(f"Recovery successful for validator {validator}, got {len(delegations)} delegations")
                        return delegations
                    
                    # Add a delay between pagination requests
                    min_delay, max_delay = pagination_delay_range
                    await asyncio.sleep(random.uniform(min_delay, max_delay))
            
            except Exception as e:
                logger.warning(f"Recovery attempt {attempt} failed for validator {validator}: {str(e)}")
                # Continue to next attempt
        
        # If we got here, we've exhausted all recovery attempts
        logger.error(f"All recovery attempts failed for validator {validator} on {chain_config.name}")
        # Return any delegations we might have collected
        return delegations
    
async def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Query staking data from Cosmos chains")
    
    parser.add_argument("--chains", type=str, help="Comma-separated list of chain names to query")
    parser.add_argument("--output", type=str, default="stakers_data.json", 
                        help="Output JSON file path (default: stakers_data.json)")
    parser.add_argument("--concurrency", type=int, default=3, 
                        help="Maximum number of chains to process concurrently (default: 3)")
    parser.add_argument("--retries", type=int, default=5, 
                        help="Maximum number of retries for API calls (default: 5)")
    parser.add_argument("--validator-retries", type=int, default=10,
                        help="Maximum number of retries for validator delegator queries (default: 10)")
    parser.add_argument("--no-recovery", action="store_true",
                        help="Disable recovery attempts for failed validators")
    parser.add_argument("--config", type=str, help="Path to JSON config file")
    
    args = parser.parse_args()
    
    # Initialize default values
    chains = []
    output_file = args.output
    concurrency_limit = args.concurrency
    max_retries = args.retries
    validator_retries = args.validator_retries
    enable_recovery = not args.no_recovery
    
    # If config file provided, load settings from it
    if args.config:
        try:
            with open(args.config, 'r') as f:
                config = json.load(f)
                
            # Load values from config, with command line args taking precedence
            if "chains" in config and isinstance(config["chains"], list):
                chains = config["chains"]
            
            if "output_file" in config and not args.output:
                output_file = config["output_file"]
                
            if "concurrency" in config and not args.concurrency:
                concurrency_limit = config["concurrency"]
                
            if "retries" in config and not args.retries:
                max_retries = config["retries"]
                
            if "validator_retries" in config and not args.validator_retries:
                validator_retries = config["validator_retries"]
                
            if "enable_recovery" in config and not args.no_recovery:
                enable_recovery = config["enable_recovery"]
                
            logger.info(f"Loaded configuration from {args.config}")
        except Exception as e:
            logger.error(f"Error loading config file: {str(e)}")
            sys.exit(1)
    
    # Command line chains arg takes precedence over config file
    if args.chains:
        chains = [chain.strip() for chain in args.chains.split(",")]
    
    # If no chains specified anywhere, use defaults
    if not chains:
        chains = DEFAULT_CHAINS
        logger.info(f"No chains specified, using defaults: {', '.join(chains)}")
    
    logger.info(f"Processing chains: {', '.join(chains)}")
    
    # Create and run the tool
    tool = CosmosStakingQueryTool(
        chains=chains,
        output_file=output_file,
        max_retries=max_retries,
        concurrency_limit=concurrency_limit,
        validator_retries=validator_retries,
        enable_recovery=enable_recovery
    )
    
    stakers_data = await tool.run()
    

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
    
   