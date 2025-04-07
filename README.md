# Cosmos Snapshot Tools

A collection of tools for gathering and processing data from Cosmos blockchains.

## Staking Query Tool

This tool queries the staking module for various Cosmos chains to gather delegator data. It:

1. Fetches chain configuration from chains.cosmos.directory
2. Queries validators from rest.cosmos.directory
3. Collects delegator data for each validator
4. Aggregates delegations by address
5. Processes multiple chains in parallel
6. Outputs comprehensive staker data in JSON format

### Features

- **Chain Discovery**: Automatically discovers chain configuration details (bech32 prefix, endpoints)
- **Pagination Handling**: Properly handles paginated responses for validators and delegators
- **Retries with Exponential Backoff**: Implements retries for API calls with exponential backoff
- **Extensive Retry Mechanism**: Ensures no validators are skipped due to temporary errors
- **Advanced Recovery Mode**: Makes additional attempts with longer delays for any validators that fail even after retries
- **Parallel Processing**: Processes multiple chains and validators concurrently
- **Comprehensive Logging**: Detailed logging with file and console output
- **JSON Output**: Outputs all staker data in a structured JSON format

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/cosmos-snapshot-tools.git
cd cosmos-snapshot-tools

# Install dependencies
pip install -r requirements.txt
```

### Usage

#### Basic Usage

Run with default chains (cosmos, osmosis, juno):

```bash
python staking_query.py
```

Run with specific chains:

```bash
python staking_query.py --chains cosmos,osmosis,juno
```

Specify a custom output file (JSON):

```bash
python staking_query.py --output my_stakers.json
```

#### Advanced Usage

Process chains with increased concurrency:

```bash
python staking_query.py --chains cosmos,osmosis --concurrency 5
```

Increase validator retries for more reliable data collection:

```bash
python staking_query.py --validator-retries 15
```

Disable recovery mode (not recommended for complete data collection):

```bash
python staking_query.py --no-recovery
```

### Command Line Options

```
--chains              Comma-separated list of chain names to query
--output              Output JSON file path (default: stakers_data.json)
--concurrency         Maximum number of chains to process concurrently (default: 3)
--retries             Maximum number of retries for API calls (default: 5)
--validator-retries   Maximum number of retries for validator delegator queries (default: 10)
--no-recovery         Disable recovery attempts for failed validators
```

### Output Format

The tool produces a JSON file with the following structure:

```json
{
  "metadata": {
    "generated_at": "2023-04-05T12:34:56",
    "chains_processed": 3,
    "total_validators_failed": 2
  },
  "chains": {
    "cosmos": {
      "chain_id": "cosmoshub-4",
      "name": "cosmos",
      "pretty_name": "Cosmos Hub",
      "bech32_prefix": "cosmos",
      "stakers": {
        "cosmos1abc...": {
          "amount": 1000000,
          "denom": "uatom"
        },
        "cosmos1xyz...": {
          "amount": 5000000,
          "denom": "uatom"
        }
      },
      "total_stakers": 1000,
      "total_staked": 500000000000,
      "denom": "uatom"
    },
    "osmosis": {
      // Similar structure for other chains
    }
  }
}
```

## Staker Filter Tool

This tool takes the JSON output from the Staking Query Tool and filters it based on various criteria, such as minimum staking thresholds.

### Usage

Filter stakers based on minimum staking thresholds:

```bash
python filter_stakers.py --input stakers_data.json --output filtered_stakers.json --min-threshold "cosmos:1000000,osmosis:5000000"
```

### Command Line Options

```
--input               Input JSON file with staker data (from staking_query.py)
--output              Output JSON file for filtered data (default: filtered_stakers.json)
--min-threshold       Minimum staking amount thresholds by chain, format: 'chain:amount,chain:amount'
```

### Examples

Include only stakers with at least 100 ATOM and 500 OSMO:

```bash
python filter_stakers.py --input stakers_data.json --min-threshold "cosmos:100000000,osmosis:500000000"
```

### Performance Considerations

- Querying large chains like Cosmos Hub or Osmosis can take a significant amount of time due to the large number of validators and delegators
- Using higher concurrency values may speed up processing but could also increase the likelihood of rate limiting by the RPC endpoints
- The script includes random delays between API calls to avoid rate limiting
- The recovery mechanism adds additional time to the process but significantly improves data completeness

### Troubleshooting

- If you encounter rate limiting, try reducing the concurrency or increasing the retry delays
- Check the log file (staking_query_YYYYMMDD_HHMMSS.log) for detailed error information
- For chains with a large number of validators/delegators, the process may take a long time to complete
- If you see many failed validators even with high retry values, the RPC endpoint may be unstable - consider trying again later

