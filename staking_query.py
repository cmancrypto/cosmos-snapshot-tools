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
from typing import Dict, List, Set, Tuple, Any, Optional

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

class ChainConfig:
    """Stores configuration data for a Cosmos chain."""
    
    def __init__(self, name: str):
        self.name = name
        self.pretty_name = ""
        self.chain_id = ""
        self.bech32_prefix = ""
        self.rest_endpoint = ""
        self.is_configured = False
    
    def __str__(self) -> str:
        return f"Chain: {self.pretty_name} ({self.name}), ID: {self.chain_id}, Prefix: {self.bech32_prefix}"


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
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch config for {chain_config.name}: HTTP {response.status}")
                    return
                
                data = await response.json()
                chain_data = data.get("chain", {})
                
                chain_config.pretty_name = chain_data.get("pretty_name", chain_config.name)
                chain_config.chain_id = chain_data.get("chain_id", "")
                chain_config.bech32_prefix = chain_data.get("bech32_prefix", "")
                
                # Get REST endpoint
                apis = chain_data.get("apis", {})
                rest_endpoints = apis.get("rest", [])
                if rest_endpoints:
                    # Prefer cosmos.directory REST endpoint if available
                    for endpoint in rest_endpoints:
                        if "cosmos.directory" in endpoint.get("address", ""):
                            chain_config.rest_endpoint = endpoint.get("address")
                            break
                    
                    # Otherwise take the first one
                    if not chain_config.rest_endpoint and rest_endpoints:
                        chain_config.rest_endpoint = rest_endpoints[0].get("address")
                
                # Double check with predefined endpoint if rest endpoint wasn't found
                if not chain_config.rest_endpoint:
                    chain_config.rest_endpoint = f"https://rest.cosmos.directory/{chain_config.name}"
                
                chain_config.is_configured = all([
                    chain_config.chain_id,
                    chain_config.bech32_prefix,
                    chain_config.rest_endpoint
                ])
                
                logger.info(f"Config for {chain_config.name}: {chain_config}")
        
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
            batch_size = 2  # Reduced from 5 to 2 validators at a time
            
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
                        delegator_data_list.extend(delegator_list)
                    
                    pbar.update(len(validator_batch))
                    
                    # Add delay between batches to avoid rate limiting
                    logger.info(f"Completed batch. Waiting before next batch...")
                    await asyncio.sleep(random.uniform(5.0, 10.0))
            
            # Record any failed validators
            if failed_validators:
                self.failed_validators[chain_config.name] = failed_validators
                logger.warning(f"Failed to process {len(failed_validators)} validators for {chain_config.name} after multiple retries")
            
            # Try to recover failed validators with extended backoff if enabled
            if failed_validators and self.enable_recovery:
                logger.info(f"Attempting recovery for {len(failed_validators)} failed validators with extended backoff")
                recovered_data = await self.recover_failed_validators(session, chain_config, failed_validators)
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
                chain_data["total_staked"] = int(stakers_df["staked_amount"].sum())
                chain_data["denom"] = stakers_df["denom"].iloc[0] if not stakers_df.empty else ""
                
                logger.info(f"Processed {len(stakers)} unique delegators for {chain_config.name}")
            else:
                logger.warning(f"No delegator data found for {chain_config.name}")
            
            return chain_data
                
        except Exception as e:
            logger.error(f"Error processing chain {chain_config.name}: {str(e)}")
            return chain_data
            
    async def recover_failed_validators(self, session: aiohttp.ClientSession, 
                                      chain_config: ChainConfig, 
                                      validators: List[str]) -> List[Dict]:
        """
        Last-ditch effort to recover data from validators that failed even after multiple retries.
        Uses much longer delays between attempts and a more conservative approach.
        """
        all_recovered_delegations = []
        
        # Group validators by whether they likely failed due to rate limiting
        rate_limited_validators = []
        other_error_validators = []
        
        for validator in validators:
            # Check if this validator was rate limited based on the error log
            if any(f"validator {validator}" in entry and "429" in entry for entry in self.get_recent_error_logs()):
                rate_limited_validators.append(validator)
            else:
                other_error_validators.append(validator)
                
        if rate_limited_validators:
            logger.info(f"Attempting recovery for {len(rate_limited_validators)} rate-limited validators with extra caution")
            
        # Process non-rate-limited validators first
        for validator in tqdm(other_error_validators, desc=f"Recovery attempts for {chain_config.name} (regular errors)"):
            recovered_delegations = await self.attempt_validator_recovery(session, chain_config, validator, 
                                                    max_attempts=3, base_delay=10)
            if recovered_delegations:
                all_recovered_delegations.extend(recovered_delegations)
                
        # Then process rate-limited validators with much more caution
        for validator in tqdm(rate_limited_validators, desc=f"Recovery attempts for {chain_config.name} (rate-limited)"):
            # Much longer delays between attempts for rate-limited validators
            recovered_delegations = await self.attempt_validator_recovery(session, chain_config, validator, 
                                                    max_attempts=3, base_delay=60, 
                                                    pagination_delay_range=(5, 15))
            if recovered_delegations:
                all_recovered_delegations.extend(recovered_delegations)
                
        return all_recovered_delegations
        
    def get_recent_error_logs(self) -> List[str]:
        """Gets recent error logs from the logger to identify rate-limited validators."""
        # This is a simple implementation; in a production environment,
        # you might want to use a more sophisticated approach to access logs
        try:
            with open(next(Path(".").glob("staking_query_*.log")), 'r') as f:
                return [line for line in f.readlines() if "ERROR" in line and ("429" in line or "rate" in line.lower())]
        except (StopIteration, FileNotFoundError, PermissionError):
            return []  # Return empty list if no log file found or can't read it
    
    async def attempt_validator_recovery(self, session: aiohttp.ClientSession, 
                                       chain_config: ChainConfig, validator: str,
                                       max_attempts: int = 3, base_delay: int = 10,
                                       pagination_delay_range: tuple = (2, 5)) -> List[Dict]:
        """Helper method to attempt recovery for a single validator with customizable parameters."""
        recovered = False
        delegations = []
        
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"Recovery attempt {attempt}/{max_attempts} for validator {validator}")
                # Extended delay before retry - increases with each attempt
                delay = base_delay * attempt
                logger.info(f"Waiting {delay}s before attempting...")
                await asyncio.sleep(delay)
                
                # Custom implementation without using the retry decorator
                next_key = None
                
                while True:
                    endpoint = f"{chain_config.rest_endpoint}/cosmos/staking/v1beta1/validators/{validator}/delegations"
                    # Use adaptive pagination size based on chain's error rate
                    pagination_size = self.get_pagination_size_for_chain(chain_config.name, "recovery")
                    params = {"pagination.limit": str(pagination_size)}
                    
                    if next_key:
                        params["pagination.key"] = next_key
                    
                    async with session.get(endpoint, params=params, timeout=60) as response:
                        if response.status == 429:
                            # If rate limited during recovery, use an even longer delay
                            error_text = await response.text()
                            retry_after = response.headers.get('Retry-After')
                            wait_time = int(retry_after) if retry_after and retry_after.isdigit() else random.randint(60, 120)
                            
                            logger.warning(f"Rate limited during recovery for {validator}. Waiting {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            break  # Break out of pagination and retry the whole validator
                            
                        if response.status != 200:
                            error_text = await response.text()
                            logger.warning(f"Recovery attempt {attempt} failed with HTTP {response.status}: {error_text}")
                            break
                        
                        data = await response.json()
                        
                        # Extract delegator info
                        for delegation in data.get("delegation_responses", []):
                            delegator_addr = delegation.get("delegation", {}).get("delegator_address")
                            balance = delegation.get("balance", {})
                            
                            if delegator_addr and balance:
                                amount = int(balance.get("amount", 0))
                                denom = balance.get("denom", "")
                                
                                if amount > 0:
                                    delegations.append({
                                        "chain": chain_config.name,
                                        "address": delegator_addr,
                                        "staked_amount": amount,
                                        "denom": denom
                                    })
                        
                        # Check for pagination
                        pagination = data.get("pagination", {})
                        next_key = pagination.get("next_key")
                        
                        if not next_key:
                            recovered = True
                            break
                        
                        # Extra delay for pagination in recovery mode - use the custom range
                        min_delay, max_delay = pagination_delay_range
                        await asyncio.sleep(random.uniform(min_delay, max_delay))
                
                if recovered:
                    logger.info(f"Successfully recovered {len(delegations)} delegations for validator {validator}")
                    break
            
            except Exception as e:
                logger.warning(f"Recovery attempt {attempt} failed for validator {validator}: {str(e)}")
        
        if not recovered:
            logger.error(f"All recovery attempts failed for validator {validator}")
            
        return delegations
    
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=60))
    async def get_all_validators(self, session: aiohttp.ClientSession, chain_config: ChainConfig) -> List[str]:
        """
        Get all validators for a chain, handling pagination.
        Returns a list of validator addresses.
        """
        validators = []
        next_key = None
        
        while True:
            endpoint = f"{chain_config.rest_endpoint}/cosmos/staking/v1beta1/validators"
            params = {"pagination.limit": "150"}
            
            if next_key:
                params["pagination.key"] = next_key
            
            try:
                async with session.get(endpoint, params=params) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Failed to get validators for {chain_config.name}: HTTP {response.status}\n{error_text}")
                        return validators
                    
                    data = await response.json()
                    
                    # Extract validator operator addresses
                    for validator in data.get("validators", []):
                        validator_addr = validator.get("operator_address")
                        if validator_addr:
                            validators.append(validator_addr)
                    
                    # Check for pagination
                    pagination = data.get("pagination", {})
                    next_key = pagination.get("next_key")
                    
                    if not next_key:
                        break
                    
                    # To avoid rate limiting
                    await asyncio.sleep(random.uniform(0.5, 1.5))
            
            except Exception as e:
                logger.error(f"Error fetching validators for {chain_config.name}: {str(e)}")
                raise
        
        return validators
    
    @retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=2, min=5, max=300))
    async def get_validator_delegations(self, session: aiohttp.ClientSession, 
                                       chain_config: ChainConfig, validator_addr: str) -> List[Dict]:
        """
        Get all delegations for a validator, handling pagination.
        Returns a list of delegator data dicts.
        
        Uses aggressive exponential backoff to handle rate limiting.
        """
        delegations = []
        next_key = None
        
        # Add a random initial delay to stagger requests
        await asyncio.sleep(random.uniform(1.0, 3.0))
        
        while True:
            endpoint = f"{chain_config.rest_endpoint}/cosmos/staking/v1beta1/validators/{validator_addr}/delegations"
            # Use adaptive pagination size based on chain's error rate
            pagination_size = self.get_pagination_size_for_chain(chain_config.name, "delegations")
            params = {"pagination.limit": str(pagination_size)}
            
            if next_key:
                params["pagination.key"] = next_key
            
            try:
                async with session.get(endpoint, params=params, timeout=60) as response:
                    # Special handling for rate limiting
                    if response.status == 429:
                        error_text = await response.text()
                        retry_after = response.headers.get('Retry-After')
                        wait_time = int(retry_after) if retry_after and retry_after.isdigit() else random.randint(30, 60)
                        
                        logger.warning(f"Rate limited (429) for validator {validator_addr} on {chain_config.name}. Waiting {wait_time}s before retry.")
                        # Record the error to potentially reduce pagination size
                        self.record_pagination_error(chain_config.name)
                        await asyncio.sleep(wait_time)
                        raise Exception(f"Rate limited: {error_text}")
                    
                    # Check for other potential pagination-related errors
                    if response.status in [500, 502, 503, 504]:
                        error_text = await response.text()
                        logger.warning(f"Server error for validator {validator_addr} on {chain_config.name}: HTTP {response.status}")
                        # Record error to potentially reduce pagination size on server errors
                        self.record_pagination_error(chain_config.name)
                        raise Exception(f"HTTP {response.status} from API: {error_text}")
                    
                    if response.status != 200:
                        # Don't immediately skip validators with error responses
                        # The @retry decorator will handle retrying this function
                        error_text = await response.text()
                        logger.error(f"Failed to get delegations for validator {validator_addr} on {chain_config.name}: HTTP {response.status}\n{error_text}")
                        raise Exception(f"HTTP {response.status} from API")
                    
                    data = await response.json()
                    
                    # Extract delegator info
                    for delegation in data.get("delegation_responses", []):
                        delegator_addr = delegation.get("delegation", {}).get("delegator_address")
                        balance = delegation.get("balance", {})
                        
                        if delegator_addr and balance:
                            amount = int(balance.get("amount", 0))
                            denom = balance.get("denom", "")
                            
                            if amount > 0:
                                delegations.append({
                                    "chain": chain_config.name,
                                    "address": delegator_addr,
                                    "staked_amount": amount,
                                    "denom": denom
                                })
                    
                    # Check for pagination
                    pagination = data.get("pagination", {})
                    next_key = pagination.get("next_key")
                    
                    if not next_key:
                        break
                    
                    # More aggressive delay for pagination to avoid rate limiting
                    await asyncio.sleep(random.uniform(3.0, 8.0))
            
            except Exception as e:
                # If we hit a rate limit, re-raise to trigger exponential backoff
                if "Rate limited" in str(e) or "429" in str(e):
                    logger.warning(f"Rate limit encountered for {validator_addr}, triggering backoff...")
                    raise
                
                logger.error(f"Error fetching delegations for validator {validator_addr} on {chain_config.name}: {str(e)}")
                raise
        
        return delegations
    
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
    
    def finalize_results(self):
        """Process the results and save to JSON file."""
        logger.info("Finalizing and saving results")
        
        try:
            # Prepare final output data
            output_data = {
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "chains_processed": len(self.all_stakers_data),
                    "total_validators_failed": sum(len(validators) for validators in self.failed_validators.values()) if self.failed_validators else 0
                },
                "chains": self.all_stakers_data
            }
            
            # Save to JSON file
            with open(self.output_file, 'w') as f:
                json.dump(output_data, f, indent=2)
            
            logger.info(f"Saved staker data to {self.output_file}")
            
            # Print summary
            total_stakers = 0
            for chain_name, chain_data in self.all_stakers_data.items():
                stakers_count = chain_data.get("total_stakers", 0)
                total_staked = chain_data.get("total_staked", 0)
                denom = chain_data.get("denom", "")
                logger.info(f"Chain {chain_name}: {stakers_count} stakers, {total_staked} {denom} total staked")
                total_stakers += stakers_count
            
            logger.info(f"Total unique stakers across {len(self.all_stakers_data)} chains: {total_stakers}")
            
            # Save report of failed validators
            if self.failed_validators:
                failed_report_path = f"failed_validators_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(failed_report_path, 'w') as f:
                    json.dump(self.failed_validators, f, indent=2)
                logger.warning(f"Some validators failed all retry attempts. See {failed_report_path} for details.")
                
                # Print summary of failed validators
                total_failed = sum(len(validators) for validators in self.failed_validators.values())
                logger.warning(f"Total failed validators: {total_failed} across {len(self.failed_validators)} chains")
        
        except Exception as e:
            logger.error(f"Error finalizing results: {str(e)}")


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