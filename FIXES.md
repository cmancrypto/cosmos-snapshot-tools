# Cosmos Snapshot Tools - Fixes and Improvements

## Issues Fixed

We identified and fixed several issues with the original implementation:

1. **Pagination Token Handling**: 
   - The original code didn't properly URL-encode pagination tokens
   - Many endpoints return base64-encoded pagination tokens that need proper encoding
   - Different endpoints handle pagination tokens differently

2. **Error Handling**:
   - Endpoint failures weren't properly tracked and handled
   - When an endpoint failed with a specific pagination token, we kept retrying the same endpoint

3. **Chain Name Variations**:
   - Some chains may have different naming conventions in chains.cosmos.directory
   - For example, "cosmoshub" might be indexed as "cosmos"

4. **Timeout Handling**:
   - No explicit timeouts were set for requests
   - Some slow endpoints never responded, causing the process to hang

## Improvements Made

### 1. Robust Pagination Token Handling

- Added proper URL encoding for all pagination tokens
- Implemented per-endpoint tracking of pagination failures
- If an endpoint fails with a specific pagination token, we try other endpoints before giving up
- Improved the validators fetching to continue with partial results if later pagination pages fail

### 2. Advanced Endpoint Management

- Enhanced the error tracking system to record which specific endpoints fail with which pagination tokens
- Implemented a more aggressive fallback mechanism that tries multiple endpoints for each request
- Added detailed logging of which endpoints are being used and their performance

### 3. Chain Configuration Improvements

- Added support for alternate chain names (e.g., trying "cosmos" if "cosmoshub" fails)
- Improved fallback mechanism with multiple potential default endpoints
- Better validation of API responses to catch malformed data

### 4. Request Handling and Timeouts

- Added explicit timeouts to all HTTP requests
- Improved error handling to catch and properly report timeout errors
- Added better recovery from transient failures

### 5. Diagnostic Tools

- Created a test script (`test_cosmos_directory.py`) to validate endpoint functionality
- The test script checks both basic connectivity and pagination support
- Generates a detailed report of working endpoints for each chain

## Using the Fixed Code

The enhanced code should work much more reliably with chains.cosmos.directory. To get the best results:

1. Run the test script first to identify working endpoints:
   ```bash
   python test_cosmos_directory.py
   ```

2. The test will create a JSON file with details on which endpoints work well

3. Then run the main script as usual:
   ```bash
   python staking_query.py --chains cosmos,osmosis,juno
   ```

4. If you still encounter issues with specific chains, try:
   - Reducing the concurrency (--concurrency 1)
   - Increasing retries (--validator-retries 15)
   - Running with a single chain at a time

## Technical Details

### URL Encoding Pagination Tokens

Many REST endpoints return base64-encoded pagination tokens that include characters like '+', '/', and '='. 
These need to be properly URL-encoded when used in subsequent requests. We added proper encoding with:

```python
encoded_key = urllib.parse.quote(next_key)
pagination_params = f"pagination.limit={pagination_size}&pagination.key={encoded_key}"
```

### Endpoint-Specific Error Tracking

Different endpoints may fail with different pagination tokens. We now track failures at a more granular level:

```python
# Record the endpoint as failing with this pagination key
if next_key:
    if next_key not in errors_by_endpoint:
        errors_by_endpoint[next_key] = []
    errors_by_endpoint[next_key].append(endpoint)
```

### Multiple Endpoint Fallback

The improved code tries multiple endpoints for each request, prioritizing those with the fewest errors:

```python
# Try up to 3 different endpoints
for attempt in range(min(3, len(chain_config.rest_endpoints))):
    # Get an endpoint that hasn't failed with this pagination key
    offset = attempt
    while offset < len(chain_config.rest_endpoints):
        endpoint = chain_config.get_rest_endpoint(offset)
        # Skip endpoints that have already failed with this key
        if next_key and endpoint in errors_by_endpoint.get(next_key, []):
            offset += 1
            continue
        break
```