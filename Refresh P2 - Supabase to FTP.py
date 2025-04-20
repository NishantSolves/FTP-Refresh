# First install required packages
!pip install supabase-py ebaysdk python-dotenv

import os
import json
import time
from supabase import create_client, Client
from ebaysdk.trading import Connection as Trading
from ebaysdk.exception import ConnectionError
from datetime import datetime
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("ebay_inventory_update.log")
    ]
)
logger = logging.getLogger()

# Function to set up Supabase client using environment variables
def setup_supabase():
    """Setup Supabase connection from environment variables"""
    # Get Supabase credentials from environment variables
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Missing Supabase credentials. Please set SUPABASE_URL and SUPABASE_KEY environment variables.")

    # Initialize Supabase client
    logger.info("Setting up Supabase connection")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    return supabase

# Function to set up eBay API connection using environment variables
def setup_ebay_api():
    """Setup eBay API connection from environment variables"""
    # Get eBay API credentials from environment variables
    EBAY_DEV_ID = os.getenv("EBAY_DEV_ID")
    EBAY_APP_ID = os.getenv("EBAY_APP_ID")
    EBAY_CERT_ID = os.getenv("EBAY_CERT_ID")
    EBAY_TOKEN = os.getenv("EBAY_TOKEN")

    if not all([EBAY_DEV_ID, EBAY_APP_ID, EBAY_CERT_ID, EBAY_TOKEN]):
        raise ValueError("Missing eBay API credentials. Please set EBAY_DEV_ID, EBAY_APP_ID, EBAY_CERT_ID, and EBAY_TOKEN environment variables.")

    # Initialize eBay API client
    logger.info("Setting up eBay API connection")
    api = Trading(
        domain='api.ebay.com',
        appid=EBAY_APP_ID,
        devid=EBAY_DEV_ID,
        certid=EBAY_CERT_ID,
        token=EBAY_TOKEN,
        config_file=None
    )

    return api

# Function to get eBay item ID using ISBN
def get_ebay_item_id(api, isbn):
    """
    Get eBay item ID using ISBN as a search term
    Returns None if the item is not found
    """
    try:
        # Search for items with this ISBN
        response = api.execute('GetSellerList', {
            'DetailLevel': 'ReturnAll',
            'SKUArray': {'SKU': isbn}
        })

        # Check if any items were found
        if hasattr(response.reply, 'ItemArray') and hasattr(response.reply.ItemArray, 'Item'):
            for item in response.reply.ItemArray.Item:
                if hasattr(item, 'SKU') and item.SKU == isbn:
                    return item.ItemID

        # If we get here, no item was found with this ISBN as SKU
        logger.warning(f"No eBay listing found for ISBN: {isbn}")
        return None

    except ConnectionError as e:
        logger.error(f"eBay API error while searching for ISBN {isbn}: {e}")
        return None

# Function to update an eBay item's price and quantity
def update_ebay_item(api, item_id, new_price, new_quantity):
    """
    Update an eBay item's price and quantity
    Returns True if successful, False otherwise
    """
    try:
        # Prepare the request to update the item
        request = {
            'Item': {
                'ItemID': item_id,
                'StartPrice': new_price,
                'Quantity': new_quantity
            }
        }

        # Execute the ReviseItem call
        response = api.execute('ReviseItem', request)

        # Check if the revision was successful
        if response.reply.Ack == 'Success' or response.reply.Ack == 'Warning':
            logger.info(f"Successfully updated eBay item {item_id}: Price={new_price}, Quantity={new_quantity}")
            return True
        else:
            logger.error(f"Failed to update eBay item {item_id}: {response.reply.Errors}")
            return False

    except ConnectionError as e:
        logger.error(f"eBay API error while updating item {item_id}: {e}")
        return False

# Function to get modified records from Supabase temporary table
def get_modified_records(supabase, temp_table_name):
    """
    Get records from the temporary table that need to be updated on eBay
    """
    try:
        # Query the temporary table for modified records
        response = supabase.table(temp_table_name).select('*').execute()

        if response.data:
            logger.info(f"Found {len(response.data)} records to update on eBay")
            return response.data
        else:
            logger.info("No records found in the temporary table for updating on eBay")
            return []

    except Exception as e:
        logger.error(f"Error retrieving modified records from Supabase: {e}")
        return []

# Function to track eBay update results
def track_update_results(supabase, temp_table_name, isbn, success, details=None):
    """
    Update the temporary table with the eBay update status
    """
    try:
        # Update the record with eBay update status
        supabase.table(temp_table_name).update({
            'ebay_updated': success,
            'ebay_update_time': datetime.now().isoformat(),
            'ebay_update_details': details
        }).eq('isbn', isbn).execute()

    except Exception as e:
        logger.error(f"Error updating eBay status in Supabase for ISBN {isbn}: {e}")

# Main function to process eBay updates
def update_ebay_inventory():
    """
    Main function to update eBay inventory based on modified records
    """
    logger.info("Starting eBay inventory update process...")

    try:
        # Get temp table name from environment variable
        temp_table_name = os.getenv("TEMP_TABLE_NAME")

        if not temp_table_name:
            raise ValueError("Missing temporary table name. Please set TEMP_TABLE_NAME environment variable.")

        logger.info(f"Using temporary table: {temp_table_name}")

        # Setup connections
        supabase = setup_supabase()
        ebay_api = setup_ebay_api()

        # Get modified records
        modified_records = get_modified_records(supabase, temp_table_name)

        if not modified_records:
            logger.info("No records to update. Exiting.")
            return

        # Process each modified record
        success_count = 0
        failure_count = 0

        for record in modified_records:
            isbn = record['isbn']
            new_stock = record['new_stock']
            new_rrp = record['new_rrp']

            logger.info(f"Processing ISBN: {isbn}, New Stock: {new_stock}, New RRP: {new_rrp}")

            # Get eBay item ID for this ISBN
            item_id = get_ebay_item_id(ebay_api, isbn)

            if not item_id:
                failure_count += 1
                track_update_results(supabase, temp_table_name, isbn, False, "Item not found on eBay")
                continue

            # Update the eBay item
            success = update_ebay_item(ebay_api, item_id, new_rrp, new_stock)

            if success:
                success_count += 1
                track_update_results(supabase, temp_table_name, isbn, True, f"Updated on eBay: Item ID {item_id}")
            else:
                failure_count += 1
                track_update_results(supabase, temp_table_name, isbn, False, f"Failed to update on eBay: Item ID {item_id}")

            # Sleep briefly to avoid eBay API rate limits
            time.sleep(1)

        # Log summary
        logger.info(f"eBay inventory update completed!")
        logger.info(f"Success: {success_count}, Failures: {failure_count}")

    except Exception as e:
        logger.error(f"Error during ebay inventory update process: {str(e)}")

# For Google Colab: Create a .env file with credentials
def setup_env_file():
    """
    Create a .env file with environment variables for Google Colab
    """
    print("Setting up environment variables for Google Colab...")

    env_content = """
# Supabase credentials
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key

# eBay API credentials
EBAY_DEV_ID=your_ebay_dev_id
EBAY_APP_ID=your_ebay_app_id
EBAY_CERT_ID=your_ebay_cert_id
EBAY_TOKEN=your_ebay_auth_token

# Temporary table name
TEMP_TABLE_NAME=your_temp_table_name
"""

    # Write the template .env file
    with open('.env', 'w') as f:
        f.write(env_content)

    print("Created .env file template. Please edit it with your actual credentials.")

    # Option to edit in Colab
    from google.colab import files

    print("\nEdit the .env file with your credentials and then upload it:")
    files.upload()

if __name__ == "__main__":
    # For Colab, create an env file template
    if 'google.colab' in str(get_ipython()):
        setup_env_file()

    # Run the main function
    update_ebay_inventory()