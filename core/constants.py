import os

# GCP Project Configuration
PROJECT_ID = "nih-nci-dceg-connect-prod-6d04"

TARGET_DATASET_ID = "NORC"
FLAT_SOURCE_DATASET_ID = "FlatConnect"
RAW_SOURCE_DATASET_ID = "Connect"

# Source Table Names
MODULE_4_TABLE = f"`{PROJECT_ID}`.{FLAT_SOURCE_DATASET_ID}.module4_v1_JP"
FLAT_PARTICIPANTS_TABLE = f"`{PROJECT_ID}`.{FLAT_SOURCE_DATASET_ID}.participants_JP"
RAW_PARTICIPANTS_TABLE = f"`{PROJECT_ID}`.{RAW_SOURCE_DATASET_ID}.participants"

# Target Table Names
METADATA_TABLE = f"`{PROJECT_ID}`.{TARGET_DATASET_ID}.address_delivery_metadata"
ADDRESSES_VIEW = f"`{PROJECT_ID}`.{TARGET_DATASET_ID}.addresses_all"
CURRENT_DELIVERY_TABLE = f"`{PROJECT_ID}`.{TARGET_DATASET_ID}.address_delivery_current"
COMPREHENSIVE_TABLE = f"`{PROJECT_ID}`.{TARGET_DATASET_ID}.address_deliveries"

# Storage Configuration
BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-default-bucket-name")
EXPORT_FOLDER = "norc_address_delivery"
LOCAL_EXPORT = True  # Set to True to export locally instead of to GCS
LOCAL_EXPORT_DIR = os.path.join(os.getcwd(), "exports")  # Better path for exports

# SQL File Paths
SQL_DIR = "sql"
ADDRESS_QUERY_SQL = "address_view.sql"
USER_PROFILE_QUERY_SQL = "user_profile_address_view.sql"

# Query Timeout (in seconds)
QUERY_TIMEOUT = 300