#!/usr/bin/env python3
"""
Cosmos Staker Data Filter

This script filters staker data from the output of staking_query.py based on 
various criteria such as minimum staking amounts per chain.
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"filter_stakers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class StakerFilter:
    """Filter for staker data from JSON output of staking_query.py."""
    
    def __init__(self, input_file: str, output_file: str):
        self.input_file = input_file
        self.output_file = output_file
        self.data = None
        self.filtered_data = None
        self.thresholds = {}  # Chain name -> minimum stake threshold
    
    def load_data(self):
        """Load data from input file."""
        try:
            with open(self.input_file, 'r') as f:
                self.data = json.load(f)
                
            if not self.data or "chains" not in self.data:
                logger.error(f"Invalid data format in {self.input_file}")
                return False
                
            # Create a copy for filtered data
            self.filtered_data = {
                "metadata": self.data.get("metadata", {}),
                "chains": {}
            }
            
            # Add filter metadata
            self.filtered_data["metadata"]["filtered_at"] = datetime.now().isoformat()
            self.filtered_data["metadata"]["original_file"] = self.input_file
            
            return True
        except Exception as e:
            logger.error(f"Error loading data from {self.input_file}: {str(e)}")
            return False
    
    def set_threshold(self, chain: str, amount: int):
        """Set minimum stake threshold for a specific chain."""
        self.thresholds[chain] = amount
        logger.info(f"Set minimum threshold for {chain} to {amount}")
    
    def apply_filters(self):
        """Apply all filters to the data."""
        if not self.data or not self.filtered_data:
            logger.error("No data loaded. Call load_data() first.")
            return False
        
        chains_data = self.data.get("chains", {})
        filtered_chains = {}
        
        # Process each chain
        for chain_name, chain_data in chains_data.items():
            # Skip chains with no stakers
            if "stakers" not in chain_data or not chain_data["stakers"]:
                logger.warning(f"No stakers data for chain {chain_name}")
                continue
            
            threshold = self.thresholds.get(chain_name, 0)
            
            # Apply threshold filter if set
            if threshold > 0:
                logger.info(f"Applying minimum threshold of {threshold} to chain {chain_name}")
                filtered_stakers = {}
                
                # Filter stakers based on minimum amount
                for address, stake_data in chain_data["stakers"].items():
                    if stake_data.get("amount", 0) >= threshold:
                        filtered_stakers[address] = stake_data
                
                # Skip chains with no stakers after filtering
                if not filtered_stakers:
                    logger.warning(f"No stakers remain for {chain_name} after applying threshold {threshold}")
                    continue
                
                # Create filtered chain data
                filtered_chain = {
                    "chain_id": chain_data.get("chain_id", ""),
                    "name": chain_name,
                    "pretty_name": chain_data.get("pretty_name", chain_name),
                    "bech32_prefix": chain_data.get("bech32_prefix", ""),
                    "stakers": filtered_stakers,
                    "total_stakers": len(filtered_stakers),
                    "total_staked": sum(s["amount"] for s in filtered_stakers.values()),
                    "denom": chain_data.get("denom", ""),
                    "filter_applied": {
                        "min_threshold": threshold
                    }
                }
                
                filtered_chains[chain_name] = filtered_chain
                logger.info(f"Chain {chain_name}: filtered from {chain_data.get('total_stakers', 0)} to {len(filtered_stakers)} stakers")
            else:
                # No threshold, include all stakers
                filtered_chains[chain_name] = chain_data
                logger.info(f"Chain {chain_name}: included all {chain_data.get('total_stakers', 0)} stakers (no threshold)")
        
        # Update filtered data
        self.filtered_data["chains"] = filtered_chains
        self.filtered_data["metadata"]["filtered_chains"] = len(filtered_chains)
        self.filtered_data["metadata"]["filter_thresholds"] = self.thresholds
        
        # Calculate total stakers across all chains
        total_stakers = sum(chain.get("total_stakers", 0) for chain in filtered_chains.values())
        self.filtered_data["metadata"]["total_stakers"] = total_stakers
        
        return True
    
    def save_filtered_data(self):
        """Save filtered data to output file."""
        if not self.filtered_data:
            logger.error("No filtered data available. Call apply_filters() first.")
            return False
        
        try:
            with open(self.output_file, 'w') as f:
                json.dump(self.filtered_data, f, indent=2)
            
            logger.info(f"Saved filtered data to {self.output_file}")
            
            # Print summary
            chains_count = len(self.filtered_data.get("chains", {}))
            total_stakers = self.filtered_data.get("metadata", {}).get("total_stakers", 0)
            
            logger.info(f"Filter summary: {chains_count} chains, {total_stakers} total stakers after filtering")
            
            for chain_name, chain_data in self.filtered_data.get("chains", {}).items():
                stakers_count = chain_data.get("total_stakers", 0)
                threshold = self.thresholds.get(chain_name, 0)
                logger.info(f"  {chain_name}: {stakers_count} stakers (min threshold: {threshold})")
            
            return True
        except Exception as e:
            logger.error(f"Error saving filtered data to {self.output_file}: {str(e)}")
            return False


def parse_threshold(threshold_str: str) -> Dict[str, int]:
    """Parse threshold string in format 'chain:amount,chain:amount'."""
    thresholds = {}
    
    if not threshold_str:
        return thresholds
    
    try:
        pairs = [pair.strip() for pair in threshold_str.split(",")]
        for pair in pairs:
            if ":" not in pair:
                logger.warning(f"Invalid threshold format: {pair}, expected 'chain:amount'")
                continue
                
            chain, amount_str = pair.split(":", 1)
            chain = chain.strip()
            
            try:
                amount = int(amount_str.strip())
                thresholds[chain] = amount
            except ValueError:
                logger.warning(f"Invalid amount for chain {chain}: {amount_str}")
    except Exception as e:
        logger.error(f"Error parsing thresholds: {str(e)}")
    
    return thresholds


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Filter staker data based on various criteria")
    
    parser.add_argument("--input", type=str, required=True,
                        help="Input JSON file with staker data (from staking_query.py)")
    parser.add_argument("--output", type=str, default="filtered_stakers.json",
                        help="Output JSON file for filtered data (default: filtered_stakers.json)")
    parser.add_argument("--min-threshold", type=str,
                        help="Minimum staking amount thresholds by chain, format: 'chain:amount,chain:amount'")
    
    args = parser.parse_args()
    
    # Create filter
    filter_tool = StakerFilter(args.input, args.output)
    
    # Load data
    if not filter_tool.load_data():
        logger.error("Failed to load data, exiting")
        sys.exit(1)
    
    # Set thresholds
    if args.min_threshold:
        thresholds = parse_threshold(args.min_threshold)
        for chain, amount in thresholds.items():
            filter_tool.set_threshold(chain, amount)
    
    # Apply filters
    if not filter_tool.apply_filters():
        logger.error("Failed to apply filters, exiting")
        sys.exit(1)
    
    # Save filtered data
    if not filter_tool.save_filtered_data():
        logger.error("Failed to save filtered data, exiting")
        sys.exit(1)
    
    logger.info("Filtering completed successfully")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
        sys.exit(1) 