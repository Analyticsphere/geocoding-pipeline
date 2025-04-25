import os
import datetime
from google.cloud import bigquery
import constants
from tabulate import tabulate
from utils import logger

def create_required_tables(client):
    """Create required tables if they don't exist"""
    logger.info("Creating required tables if they don't exist")
    
    # Create metadata table - add address_hash column
    metadata_table = constants.METADATA_TABLE
    metadata_table_query = f"""
    CREATE TABLE IF NOT EXISTS {metadata_table} (
        delivery_id STRING,
        delivery_date TIMESTAMP,
        Connect_ID STRING,
        address_src_question_cid STRING,
        address_nickname STRING,
        address_hash STRING,  -- Add address_hash column
        ts_address_delivered TIMESTAMP
    )
    """
    
    # Create comprehensive table - add the new columns
    comprehensive_table = constants.COMPREHENSIVE_TABLE
    comprehensive_query = f"""
    CREATE TABLE IF NOT EXISTS {comprehensive_table} (
        delivery_id STRING,
        delivery_date TIMESTAMP,
        Connect_ID STRING,
        ts_user_profile_updated TIMESTAMP,
        address_src_question_cid STRING,
        address_nickname STRING,
        address_source STRING,
        ts_address_delivered TIMESTAMP,
        historical_order INT64,
        address_line_1 STRING,
        address_line_2 STRING,
        street_num STRING,
        street_name STRING,
        apartment_num STRING,
        city STRING,
        state STRING,
        zip_code STRING,
        country STRING,
        cross_street_1 STRING,
        cross_street_2 STRING
    )
    """
    
    # Create current delivery table - add the new columns
    current_delivery_table = constants.CURRENT_DELIVERY_TABLE
    current_delivery_query = f"""
    CREATE TABLE IF NOT EXISTS {current_delivery_table} (
        delivery_id STRING,
        delivery_date TIMESTAMP,
        Connect_ID STRING,
        ts_user_profile_updated TIMESTAMP,
        address_src_question_cid STRING,
        address_nickname STRING,
        address_source STRING,
        ts_address_delivered TIMESTAMP,
        historical_order INT64,
        address_line_1 STRING,
        address_line_2 STRING,
        street_num STRING,
        street_name STRING,
        apartment_num STRING,
        city STRING,
        state STRING,
        zip_code STRING,
        country STRING,
        cross_street_1 STRING,
        cross_street_2 STRING
    )
    """
    
    # Execute queries
    client.query(metadata_table_query, timeout=constants.QUERY_TIMEOUT).result()
    client.query(comprehensive_query, timeout=constants.QUERY_TIMEOUT).result()
    client.query(current_delivery_query, timeout=constants.QUERY_TIMEOUT).result()
    
    logger.info("Required tables created/verified")

def create_address_view(client):
    """Create or update the address view"""
    logger.info("Creating/updating address view")
    
    # Get the formatted SQL for both queries separately
    module4_sql_path = os.path.join(constants.SQL_DIR, constants.ADDRESS_QUERY_SQL)
    user_profile_sql_path = os.path.join(constants.SQL_DIR, constants.USER_PROFILE_QUERY_SQL)
    
    with open(module4_sql_path, 'r') as f:
        module4_query = f.read()
    
    with open(user_profile_sql_path, 'r') as f:
        user_profile_query = f.read()
    
    # Replace placeholders
    module4_query = module4_query.replace('@flat_module4', constants.MODULE_4_TABLE)
    module4_query = module4_query.replace('@flat_participants', constants.FLAT_PARTICIPANTS_TABLE)
    module4_query = module4_query.replace('@raw_participants', constants.RAW_PARTICIPANTS_TABLE)
    
    user_profile_query = user_profile_query.replace('@flat_module4', constants.MODULE_4_TABLE)
    user_profile_query = user_profile_query.replace('@flat_participants', constants.FLAT_PARTICIPANTS_TABLE)
    user_profile_query = user_profile_query.replace('@raw_participants', constants.RAW_PARTICIPANTS_TABLE)
    
    # Defensively strip the ; from the two queries.. 
    module4_query = module4_query.strip().rstrip(';')
    user_profile_query = user_profile_query.strip().rstrip(';')

    # Create the combined view
    view_name = constants.ADDRESSES_VIEW
    combined_query = f"""
    {module4_query}
    UNION ALL
    {user_profile_query}
    """

    
    view_query = f"""
CREATE OR REPLACE VIEW {view_name} AS
SELECT * FROM (
    {combined_query}
)
WHERE
    (street_num IS NOT NULL
    OR street_name IS NOT NULL
    OR apartment_num IS NOT NULL
    OR city IS NOT NULL
    OR state IS NOT NULL
    OR zip_code IS NOT NULL
    OR country IS NOT NULL
    OR cross_street_1 IS NOT NULL
    OR cross_street_2 IS NOT NULL
    OR address_line_1 IS NOT NULL
    OR address_line_2 IS NOT NULL)
    AND Connect_ID IN (
        SELECT CAST(Connect_ID AS STRING)
        FROM {constants.RAW_PARTICIPANTS_TABLE}
        WHERE
            d_821247024 = 197316935 -- Verification status = verified
            AND d_831041022 = 104430631 -- Data destruction requested = no
            AND d_663265240 = 231311385 -- Module 4 is complete
        )
ORDER BY Connect_ID, address_nickname
"""

    # Save the query for debugging
    debug_dir = os.path.join(os.getcwd(), 'debug')
    os.makedirs(debug_dir, exist_ok=True)
    
    with open(os.path.join(debug_dir, 'view_query.sql'), 'w') as f:
        f.write(view_query)
    
    logger.info(f"SQL query saved to {os.path.join(debug_dir, 'view_query.sql')} for debugging")
    
    # Execute the query
    try:
        job_config = bigquery.QueryJobConfig()
        job = client.query(view_query, job_config=job_config, timeout=constants.QUERY_TIMEOUT)
        job.result()
        logger.info(f"Address view {view_name} created/updated successfully")
    except Exception as e:
        logger.error(f"Error creating view: {str(e)}")
        logger.info("Check the debug file for the SQL query that failed")
        raise

def identify_new_addresses(client, delivery_id):
    """Identify new addresses that haven't been delivered yet"""
    logger.info(f"Identifying new addresses for delivery ID: {delivery_id}")
    
    metadata_table = constants.METADATA_TABLE
    addresses_view = constants.ADDRESSES_VIEW
    current_delivery_table = constants.CURRENT_DELIVERY_TABLE
    
    # Step 1: Get list of new addresses using a hash for uniqueness
    find_query = f"""
    WITH address_hashes AS (
      SELECT
        Connect_ID,
        address_src_question_cid,
        address_nickname,
        -- Create a hash of all address fields to uniquely identify each address
        TO_HEX(MD5(CONCAT(
          IFNULL(Connect_ID, ''),
          IFNULL(address_src_question_cid, ''),
          IFNULL(address_nickname, ''),
          IFNULL(address_line_1, ''),
          IFNULL(address_line_2, ''),
          IFNULL(street_num, ''),
          IFNULL(street_name, ''),
          IFNULL(apartment_num, ''),
          IFNULL(city, ''),
          IFNULL(state, ''),
          IFNULL(zip_code, ''),
          IFNULL(country, ''),
          IFNULL(cross_street_1, ''),
          IFNULL(cross_street_2, '')
        ))) AS address_hash
      FROM {addresses_view}
    ),
    already_delivered_hashes AS (
      SELECT DISTINCT address_hash
      FROM {metadata_table}
    )
    
    SELECT
      a.*,
      h.address_hash,
      @delivery_id AS delivery_id,
      CURRENT_TIMESTAMP() AS delivery_date
    FROM {addresses_view} a
    JOIN address_hashes h
      ON a.Connect_ID = h.Connect_ID 
      AND a.address_src_question_cid = h.address_src_question_cid
      AND a.address_nickname = h.address_nickname
    LEFT JOIN already_delivered_hashes d
      ON h.address_hash = d.address_hash
    WHERE d.address_hash IS NULL
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("delivery_id", "STRING", delivery_id)
        ]
    )
    
    # Run query to find new addresses
    temp_table_id = f"{constants.PROJECT_ID}.{constants.TARGET_DATASET_ID}.temp_new_addresses_{delivery_id.replace('-', '_').lower()}"
    
    # Use destination table to store results
    job_config.destination = temp_table_id
    job_config.write_disposition = "WRITE_TRUNCATE"
    
    job = client.query(find_query, job_config=job_config, timeout=constants.QUERY_TIMEOUT)
    job.result()
    
    # Step 2: Create the final table using the temp table (without parameters)
    create_query = f"""
    CREATE OR REPLACE TABLE {current_delivery_table} AS
    SELECT * FROM `{temp_table_id}`
    """
    
    client.query(create_query, timeout=constants.QUERY_TIMEOUT).result()
    
    # Clean up the temporary table
    client.delete_table(temp_table_id, not_found_ok=True)
    
    # Count the new addresses
    count_query = f"SELECT COUNT(*) as count FROM {current_delivery_table}"
    count_job = client.query(count_query, timeout=constants.QUERY_TIMEOUT)
    result = list(count_job.result())[0]
    count = result['count']
    
    logger.info(f"Found {count} new addresses")
    return count

def update_metadata(client, delivery_id):
    """Update metadata and comprehensive tables with new addresses"""
    logger.info(f"Updating metadata for delivery ID: {delivery_id}")
    
    metadata_table = constants.METADATA_TABLE
    comprehensive_table = constants.COMPREHENSIVE_TABLE
    current_delivery_table = constants.CURRENT_DELIVERY_TABLE
    
    # Insert into metadata table - include address_hash
    metadata_query = f"""
    INSERT INTO {metadata_table} (
      delivery_id,
      delivery_date,
      Connect_ID,
      address_src_question_cid,
      address_nickname,
      address_hash,  -- Include address_hash in metadata
      ts_address_delivered
    )
    SELECT 
      delivery_id,
      delivery_date,
      Connect_ID,
      address_src_question_cid,
      address_nickname,
      address_hash,  -- Include address_hash in the SELECT
      ts_address_delivered
    FROM {current_delivery_table}
    """
    
    # Get the schema of the comprehensive table
    schema_query = f"SELECT column_name, data_type FROM `{constants.PROJECT_ID}`.{constants.TARGET_DATASET_ID}.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{constants.COMPREHENSIVE_TABLE.replace('`', '').split('.')[-1]}' ORDER BY ordinal_position"
    schema_job = client.query(schema_query, timeout=constants.QUERY_TIMEOUT)
    
    # Build the column list with explicit CAST statements if needed
    column_list = []
    select_list = []
    
    for row in schema_job.result():
        column_name = row.column_name
        column_list.append(column_name)
        
        # Check if the column exists in current_delivery_table
        if column_name in ["delivery_id", "delivery_date", "Connect_ID", "ts_user_profile_updated",
                          "address_src_question_cid", "address_nickname", "address_source",
                          "ts_address_delivered", "historical_order", 
                          "address_line_1", "address_line_2", "street_num", "street_name", 
                          "apartment_num", "city", "state", "zip_code", "country", 
                          "cross_street_1", "cross_street_2"]:
            # Add proper CAST to ensure type compatibility
            select_list.append(f"CAST({column_name} AS {row.data_type}) AS {column_name}")
        else:
            # For columns not in current_delivery_table, use NULL with proper casting
            select_list.append(f"CAST(NULL AS {row.data_type}) AS {column_name}")
    
    # Create the column list strings for the query
    columns_str = ", ".join(column_list)
    select_str = ", ".join(select_list)
    
    # Insert into comprehensive table with explicit column lists and type casting
    comprehensive_query = f"""
    INSERT INTO {comprehensive_table} (
      {columns_str}
    )
    SELECT
      {select_str}
    FROM {current_delivery_table}
    """
    
    job_config = bigquery.QueryJobConfig()
    client.query(metadata_query, job_config=job_config, timeout=constants.QUERY_TIMEOUT).result()
    client.query(comprehensive_query, job_config=job_config, timeout=constants.QUERY_TIMEOUT).result()
    
    logger.info("Metadata updated successfully")

def export_addresses(client, delivery_id, local_export=False, local_dir=None):
    """
    Export addresses either to a GCS bucket or locally
    
    Args:
        client: BigQuery client
        delivery_id: ID for this delivery
        local_export: If True, export to local file instead of GCS
        local_dir: Directory to save local files (optional)
    """
    logger.info(f"Exporting addresses for delivery ID: {delivery_id}")
    
    current_delivery_table = constants.CURRENT_DELIVERY_TABLE
    delivery_date = datetime.datetime.now().strftime('%Y%m%d')
    
    if not local_export:
        # Export to GCS bucket
        export_location = f'gs://{constants.BUCKET_NAME}/{constants.EXPORT_FOLDER}/{delivery_date}/'
        
        export_query = f"""
        EXPORT DATA
        OPTIONS (
          uri = @export_uri,
          format = 'CSV',
          overwrite = true,
          header = true,
          field_delimiter = ','
        ) AS (
          SELECT * FROM {current_delivery_table}
        )
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("export_uri", "STRING", f"{export_location}*.csv")
            ]
        )
        
        client.query(export_query, job_config=job_config, timeout=constants.QUERY_TIMEOUT).result()
        
        logger.info(f"Addresses exported successfully to {export_location}")
        return export_location
    else:
        # Export to local file
        logger.info("Exporting to local file...")
        
        # Determine local directory
        if local_dir is None:
            local_dir = os.path.join(os.getcwd(), 'exports', delivery_date)
        
        # Create directory if it doesn't exist
        os.makedirs(local_dir, exist_ok=True)
        
        # Get the data
        query = f"SELECT * FROM {current_delivery_table}"
        job_config = bigquery.QueryJobConfig()
        df = client.query(query, job_config=job_config, timeout=constants.QUERY_TIMEOUT).to_dataframe()
        
        # Save to CSV
        local_file_path = os.path.join(local_dir, f'norc_addresses_{delivery_date}.csv')
        df.to_csv(local_file_path, index=False)
        
        logger.info(f"Addresses exported successfully to {local_file_path}")
        return local_file_path
    
def delete_delivery(client, delivery_id):
    """
    Delete a delivery from the metadata tables
    
    Args:
        client: BigQuery client
        delivery_id: ID of the delivery to delete
    """
    logger.info(f"Deleting delivery ID: {delivery_id}")
    
    metadata_table = constants.METADATA_TABLE
    comprehensive_table = constants.COMPREHENSIVE_TABLE
    
    # Delete from metadata table
    metadata_delete_query = f"""
    DELETE FROM {metadata_table}
    WHERE delivery_id = @delivery_id
    """
    
    # Delete from comprehensive table
    comprehensive_delete_query = f"""
    DELETE FROM {comprehensive_table}
    WHERE delivery_id = @delivery_id
    """
    
    # Execute queries
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("delivery_id", "STRING", delivery_id)
        ]
    )
    
    try:
        # Delete from metadata table
        client.query(metadata_delete_query, job_config=job_config, timeout=constants.QUERY_TIMEOUT).result()
        logger.info(f"Deleted delivery {delivery_id} from metadata table")
        
        # Delete from comprehensive table
        client.query(comprehensive_delete_query, job_config=job_config, timeout=constants.QUERY_TIMEOUT).result()
        logger.info(f"Deleted delivery {delivery_id} from comprehensive table")
        
        logger.info(f"Successfully deleted delivery: {delivery_id}")
    except Exception as e:
        logger.error(f"Error deleting delivery {delivery_id}: {str(e)}")
        raise

def generate_summary_statistics(client, delivery_id=None):
    """
    Generate summary statistics for addresses and print them as ASCII tables
    
    Args:
        client: BigQuery client
        delivery_id: Optional ID to filter for a specific delivery
    
    Returns:
        Dictionary containing summary statistics
    """
    logger.info("Generating summary statistics...")
    
    # Define the tables we'll query
    comprehensive_table = constants.COMPREHENSIVE_TABLE
    
    # Base query for when delivery_id is provided
    delivery_filter = f"WHERE delivery_id = @delivery_id" if delivery_id else ""
    delivery_param = [bigquery.ScalarQueryParameter("delivery_id", "STRING", delivery_id)] if delivery_id else []
    
    # Query 1: Total addresses and unique participants
    count_query = f"""
    SELECT 
        COUNT(*) AS total_addresses,
        COUNT(DISTINCT Connect_ID) AS total_participants,
        COUNT(*) / COUNT(DISTINCT Connect_ID) AS avg_addresses_per_participant
    FROM {comprehensive_table}
    {delivery_filter}
    """
    
    # Query 2: Addresses by address_nickname
    nickname_query = f"""
    SELECT 
        address_nickname,
        COUNT(*) AS count,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS percentage
    FROM {comprehensive_table}
    {delivery_filter}
    GROUP BY address_nickname
    ORDER BY count DESC
    """
    
    # Query 3: Addresses by address_source
    source_query = f"""
    SELECT 
        address_source,
        COUNT(*) AS count,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS percentage
    FROM {comprehensive_table}
    {delivery_filter}
    GROUP BY address_source
    ORDER BY count DESC
    """
    
    # Query 4: Distribution of addresses per participant
    distribution_query = f"""
    WITH participant_counts AS (
        SELECT 
            Connect_ID, 
            COUNT(*) AS address_count
        FROM {comprehensive_table}
        {delivery_filter}
        GROUP BY Connect_ID
    )
    SELECT 
        address_count,
        COUNT(*) AS participant_count,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS percentage
    FROM participant_counts
    GROUP BY address_count
    ORDER BY address_count
    """
    
    # Query 5: Completeness of address fields
    completeness_query = f"""
    SELECT
        COUNT(*) AS total_addresses,
        COUNTIF(street_num IS NOT NULL) AS street_num_count,
        COUNTIF(street_name IS NOT NULL) AS street_name_count,
        COUNTIF(apartment_num IS NOT NULL) AS apartment_num_count,
        COUNTIF(city IS NOT NULL) AS city_count,
        COUNTIF(state IS NOT NULL) AS state_count,
        COUNTIF(zip_code IS NOT NULL) AS zip_code_count,
        COUNTIF(country IS NOT NULL) AS country_count,
        COUNTIF(cross_street_1 IS NOT NULL) AS cross_street_1_count,
        COUNTIF(cross_street_2 IS NOT NULL) AS cross_street_2_count,
        COUNTIF(address_line_1 IS NOT NULL) AS address_line_1_count,
        COUNTIF(address_line_2 IS NOT NULL) AS address_line_2_count
    FROM {comprehensive_table}
    {delivery_filter}
    """
    
    # Execute queries
    job_config = bigquery.QueryJobConfig(query_parameters=delivery_param)
    
    count_result = client.query(count_query, job_config=job_config).result()
    nickname_result = client.query(nickname_query, job_config=job_config).result()
    source_result = client.query(source_query, job_config=job_config).result()
    distribution_result = client.query(distribution_query, job_config=job_config).result()
    completeness_result = client.query(completeness_query, job_config=job_config).result()
    
    # Convert results to dictionaries
    count_stats = list(count_result)[0]
    nickname_stats = [dict(row) for row in nickname_result]
    source_stats = [dict(row) for row in source_result]
    distribution_stats = [dict(row) for row in distribution_result]
    completeness_stats = list(completeness_result)[0]
    
    # Calculate percentages for field completeness
    field_completeness = []
    total = completeness_stats.total_addresses
    if total > 0:
        for field in ['street_num', 'street_name', 'apartment_num', 'city', 'state', 
                    'zip_code', 'country', 'cross_street_1', 'cross_street_2',
                    'address_line_1', 'address_line_2']:
            count = getattr(completeness_stats, f"{field}_count")
            percentage = (count / total) * 100
            field_completeness.append({
                'field': field,
                'count': count,
                'percentage': percentage
            })
    
    # Compile all stats into a dictionary
    stats = {
        "delivery_id": delivery_id if delivery_id else "All deliveries",
        "total_addresses": count_stats.total_addresses,
        "total_participants": count_stats.total_participants,
        "avg_addresses_per_participant": count_stats.avg_addresses_per_participant,
        "addresses_by_nickname": nickname_stats,
        "addresses_by_source": source_stats,
        "addresses_per_participant_distribution": distribution_stats,
        "field_completeness": field_completeness
    }
    
    # Print a formatted report with ASCII tables
    delivery_info = f" for delivery {delivery_id}" if delivery_id else ""
    print(f"\n========== Summary Statistics{delivery_info} ==========\n")
    
    # Table 1: Overview
    overview_data = [
        ["Total Addresses", stats['total_addresses']],
        ["Total Participants", stats['total_participants']],
        ["Average Addresses per Participant", f"{stats['avg_addresses_per_participant']:.2f}"]
    ]
    print(tabulate(overview_data, headers=["Metric", "Value"], tablefmt="grid"))
    print("\n")
    
    # Table 2: Addresses by Nickname
    nickname_data = [[item['address_nickname'], item['count'], f"{item['percentage']:.1f}%"] 
                     for item in stats['addresses_by_nickname']]
    print(tabulate(nickname_data, headers=["Address Nickname", "Count", "Percentage"], tablefmt="grid"))
    print("\n")
    
    # Table 3: Addresses by Source
    source_data = [[item['address_source'], item['count'], f"{item['percentage']:.1f}%"] 
                   for item in stats['addresses_by_source']]
    print(tabulate(source_data, headers=["Address Source", "Count", "Percentage"], tablefmt="grid"))
    print("\n")
    
    # Table 4: Distribution of Addresses per Participant
    distribution_data = [[item['address_count'], item['participant_count'], f"{item['percentage']:.1f}%"] 
                         for item in stats['addresses_per_participant_distribution']]
    print(tabulate(distribution_data, 
                   headers=["Addresses per Participant", "Number of Participants", "Percentage"], 
                   tablefmt="grid"))
    print("\n")
    
    # Table 5: Field Completeness
    completeness_data = [[item['field'], item['count'], f"{item['percentage']:.1f}%"] 
                         for item in stats['field_completeness']]
    print(tabulate(completeness_data, headers=["Field", "Count", "Percentage"], tablefmt="grid"))
    print("\n")
    
    return stats