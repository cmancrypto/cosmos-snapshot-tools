# Cosmos Snapshot Tools

## Staking Query Tool

This tool queries the staking module for various Cosmos chains to gather delegator data for airdrop allocation calculations. It:

1. Fetches chain configuration from chains.cosmos.directory
2. Queries validators from rest.cosmos.directory
3. Collects delegator data for each validator
4. Aggregates delegations by address
5. Processes multiple chains in parallel

### Features

- **Chain Discovery**: Automatically discovers chain configuration details (bech32 prefix, endpoints)
- **Pagination Handling**: Properly handles paginated responses for validators and delegators
- **Retries with Exponential Backoff**: Implements retries for API calls with exponential backoff
- **Extensive Retry Mechanism**: Ensures no validators are skipped due to temporary errors
- **Advanced Recovery Mode**: Makes additional attempts with longer delays for any validators that fail even after retries
- **Parallel Processing**: Processes multiple chains and validators concurrently
- **Comprehensive Logging**: Detailed logging with file and console output
- **Flexible Configuration**: Define chains via command line, config file, or defaults

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

Run with default chains:

```bash
python staking_query.py
```

Run with specific chains:

```bash
python staking_query.py --chains cosmos,osmosis,juno
```

#### Advanced Usage

Use a configuration file for chain allocations:

```bash
python staking_query.py --config example_config.json
```

Specify output file and concurrency level:

```bash
python staking_query.py --chains cosmos,osmosis --output cosmos_delegators.csv --concurrency 5
```

Increase validator retries for more reliable data collection:

```bash
python staking_query.py --validator-retries 15
```

Disable recovery mode (not recommended for complete data collection):

```bash
python staking_query.py --no-recovery
```

### Configuration File Format

Create a JSON file with chain names and token allocations:

```json
{
    "cosmos": 1000000,
    "osmosis": 2000000,
    "juno": 500000
}
```

### Command Line Options

```
--chains              Comma-separated list of chain names to query
--config              Path to JSON config file with chain:allocation pairs
--output              Output CSV file path (default: delegator_data.csv)
--concurrency         Maximum number of chains to process concurrently (default: 3)
--retries             Maximum number of retries for API calls (default: 5)
--validator-retries   Maximum number of retries for validator delegator queries (default: 10)
--no-recovery         Disable recovery attempts for failed validators
```

### Output Format

The tool produces a CSV file with the following columns:

- `chain`: The name of the chain (e.g., "cosmos", "osmosis")
- `address`: The delegator's address
- `staked_amount`: The total amount staked by the delegator (in the chain's base units)

If any validators fail even after all retry attempts, a JSON file with details about the failed validators will be created.

### Examples

```bash
# Query just the Cosmos Hub
python staking_query.py --chains cosmos

# Query multiple chains with custom output file
python staking_query.py --chains cosmos,osmosis,juno --output delegations.csv

# Use configuration file and increase concurrency
python staking_query.py --config chains.json --concurrency 5

# Maximum reliability mode with increased retries
python staking_query.py --retries 10 --validator-retries 20
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

