import datetime
from google.cloud import bigquery
import constants
from utils import logger
import address_processing

def main():
    # Generate a delivery ID
    delivery_id = f"DELIVERY_{datetime.datetime.now().strftime('%Y%m%d')}"
    
    logger.info(f"Starting geocoding pipeline with delivery ID: {delivery_id}")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=constants.PROJECT_ID)
    
    try:
        # Step 0: Create required tables if they don't exist
        address_processing.create_required_tables(client)

        # Step 1: Create/update the address view
        address_processing.create_address_view(client)
        
        # Step 2: Identify new addresses
        count = address_processing.identify_new_addresses(client, delivery_id)
        
        # If no new addresses, stop here
        if count == 0:
            logger.info("No new addresses found. Pipeline complete.")
            return
        
        # Step 3: Update metadata
        address_processing.update_metadata(client, delivery_id)
        
        # Step 4: Export addresses
        export_location = address_processing.export_addresses(
            client, 
            delivery_id,
            local_export=constants.LOCAL_EXPORT,
            local_dir=constants.LOCAL_EXPORT_DIR
        )
        
        logger.info(f"Pipeline completed successfully: {count} addresses exported to {export_location}")

        # Step 5: Generate summary statistics for this delivery
        logger.info("Generating summary statistics for this delivery...")
        address_processing.generate_summary_statistics(client, delivery_id)
        
    except Exception as e:
        logger.error(f"Error in pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    main()