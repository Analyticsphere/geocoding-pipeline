# NIH Connect Geocoding Pipeline

## Overview

This repository contains the Geocoding Pipeline for the NIH Connect study, designed to collect and prepare participant address data for geocoding by NORC. The pipeline extracts addresses from the Connect study database, manages delivery of new addresses to NORC for geocoding, and maintains metadata to track which addresses have been processed.

## Features

- Extracts addresses from both Module 4 questionnaire and User Profile data
- Filters for participants who meet specific criteria (verified status, no data destruction request, Module 4 complete)
- Identifies new addresses that haven't been processed yet
- Exports addresses in CSV format for delivery to NORC
- Maintains comprehensive metadata of all addresses processed
- Generates detailed statistics on address data quality and coverage
- Supports deletion of specific deliveries if needed

## Configuration

The pipeline is configured via the `constants.py` file:

- `PROJECT_ID`: Google Cloud Project ID
- `TARGET_DATASET_ID`: BigQuery dataset for storing pipeline tables
- `FLAT_SOURCE_DATASET_ID`: Flat Connect dataset location
- `RAW_SOURCE_DATASET_ID`: Raw Connect dataset location
- `MODULE_4_TABLE`: Module 4 questionnaire table
- `FLAT_PARTICIPANTS_TABLE`: Flattened participants table
- `RAW_PARTICIPANTS_TABLE`: Raw participants table
- `METADATA_TABLE`: Table for storing delivery metadata
- `ADDRESSES_VIEW`: View that combines all address sources
- `CURRENT_DELIVERY_TABLE`: Table for current delivery
- `COMPREHENSIVE_TABLE`: Comprehensive history of all delivered addresses
- `LOCAL_EXPORT`: Boolean to toggle between local file export and GCS export
- `LOCAL_EXPORT_DIR`: Directory for local file exports
- `SQL_DIR`: Directory containing SQL query files
- `QUERY_TIMEOUT`: Timeout for BigQuery operations (seconds)

## SQL Query Files

- `address_view.sql`: Queries that extract addresses from Module 4
- `user_profile_address_view.sql`: Queries that extract addresses from User Profile

## Running the Pipeline

To run the full pipeline:

```
python main.py
```

This will:
1. Create required tables if they don't exist
2. Create/update the address view
3. Identify addresses that haven't been delivered yet
4. Update metadata tables
5. Export addresses to a CSV file
6. Generate summary statistics

## Managing Deliveries

To delete a specific delivery:

```python
from google.cloud import bigquery
import constants
import address_processing

client = bigquery.Client(project=constants.PROJECT_ID)
address_processing.delete_delivery(client, "DELIVERY_20250424")
```

To generate statistics for a specific delivery:

```python
from google.cloud import bigquery
import constants
import address_processing

client = bigquery.Client(project=constants.PROJECT_ID)
address_processing.generate_summary_statistics(client, "DELIVERY_20250424")
```

## Pipeline Architecture

- `main.py`: Entry point that orchestrates the pipeline
- `constants.py`: Configuration parameters
- `utils.py`: Utility functions like logging
- `address_processing.py`: Core pipeline functionality

Key functions in `address_processing.py`:
- `create_required_tables()`: Creates the necessary tables if they don't exist
- `create_address_view()`: Creates or updates the address view
- `identify_new_addresses()`: Identifies addresses not yet delivered
- `update_metadata()`: Updates metadata tables with new delivery information
- `export_addresses()`: Exports addresses to CSV
- `delete_delivery()`: Deletes a specific delivery from metadata
- `generate_summary_statistics()`: Generates statistics about addresses

## Future Extensions

The pipeline is designed to be extended to:
- Ingest geocoded data back from NORC
- Store and manage latitude, longitude, and cleaned up address data
- Provide quality reports on the geocoding results
- Support additional address sources as they become available

## Monitoring and Debugging

- The pipeline generates detailed logs during execution
- Debug SQL queries are saved to a `debug` directory
- Summary statistics are displayed at the end of each successful run