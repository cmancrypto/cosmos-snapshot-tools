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
            batch_size = 5  # Process 5 validators at a time
            
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
        Uses much longer delays between attempts.
        """
        all_recovered_delegations = []
        
        for validator in tqdm(validators, desc=f"Recovery attempts for {chain_config.name}"):
            recovered = False
            # Try with much longer delays
            for attempt in range(1, 4):  # 3 additional attempts with longer delays
                try:
                    logger.info(f"Recovery attempt {attempt}/3 for validator {validator}")
                    # Extended delay before retry
                    await asyncio.sleep(10 * attempt)  # 10, 20, 30 seconds delay
                    
                    # Custom implementation without using the retry decorator
                    delegations = []
                    next_key = None
                    
                    while True:
                        endpoint = f"{chain_config.rest_endpoint}/cosmos/staking/v1beta1/validators/{validator}/delegations"
                        params = {"pagination.limit": "100"}
                        
                        if next_key:
                            params["pagination.key"] = next_key
                        
                        async with session.get(endpoint, params=params, timeout=30) as response:
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
                                break
                            
                            # Extra delay for pagination in recovery mode
                            await asyncio.sleep(random.uniform(2, 5))
                    
                    if delegations:
                        all_recovered_delegations.extend(delegations)
                        logger.info(f"Successfully recovered {len(delegations)} delegations for validator {validator}")
                        recovered = True
                        break
                
                except Exception as e:
                    logger.warning(f"Recovery attempt {attempt} failed for validator {validator}: {str(e)}")
            
            if not recovered:
                logger.error(f"All recovery attempts failed for validator {validator}")
        
        return all_recovered_delegations
    
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
            params = {"pagination.limit": "100"}
            
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
    
    @retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=120))
    async def get_validator_delegations(self, session: aiohttp.ClientSession, 
                                       chain_config: ChainConfig, validator_addr: str) -> List[Dict]:
        """
        Get all delegations for a validator, handling pagination.
        Returns a list of delegator data dicts.
        """
        delegations = []
        next_key = None
        
        while True:
            endpoint = f"{chain_config.rest_endpoint}/cosmos/staking/v1beta1/validators/{validator_addr}/delegations"
            params = {"pagination.limit": "100"}
            
            if next_key:
                params["pagination.key"] = next_key
            
            try:
                async with session.get(endpoint, params=params) as response:
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
                    
                    # To avoid rate limiting
                    await asyncio.sleep(random.uniform(0.5, 1.5))
            
            except Exception as e:
                logger.error(f"Error fetching delegations for validator {validator_addr} on {chain_config.name}: {str(e)}")
                raise
        
        return delegations
    
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
    
    args = parser.parse_args()
    
    # Determine chains to query
    chains = []
    if args.chains:
        chains = [chain.strip() for chain in args.chains.split(",")]
    else:
        chains = DEFAULT_CHAINS
        logger.info(f"No chains specified, using defaults: {', '.join(chains)}")
    
    logger.info(f"Processing chains: {', '.join(chains)}")
    
    # Create and run the tool
    tool = CosmosStakingQueryTool(
        chains=chains,
        output_file=args.output,
        max_retries=args.retries,
        concurrency_limit=args.concurrency,
        validator_retries=args.validator_retries,
        enable_recovery=not args.no_recovery
    )
    
    stakers_data = await tool.run()
    

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True) 