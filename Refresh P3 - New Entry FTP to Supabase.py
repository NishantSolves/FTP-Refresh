# Install required packages
!pip install supabase-py python-dotenv ftplib pandas

import os
import io
import csv
import glob
import pandas as pd
import ftplib
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
import logging
import tempfile

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("new_isbn_discovery.log")
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

# Function to connect to FTP server using environment variables
def connect_to_ftp():
    """Connect to FTP server using environment variables"""
    # Get FTP credentials from environment variables
    FTP_HOST = os.getenv("FTP_HOST")
    FTP_USER = os.getenv("FTP_USER")
    FTP_PASS = os.getenv("FTP_PASS")
    FTP_PATH = os.getenv("FTP_PATH", "/")  # Default to root if not specified

    if not all([FTP_HOST, FTP_USER, FTP_PASS]):
        raise ValueError("Missing FTP credentials. Please set FTP_HOST, FTP_USER, and FTP_PASS environment variables.")

    try:
        # Connect to FTP server
        logger.info(f"Connecting to FTP server: {FTP_HOST}")
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)

        # Change to specified directory if provided
        if FTP_PATH != "/":
            ftp.cwd(FTP_PATH)
            logger.info(f"Changed to directory: {FTP_PATH}")

        return ftp
    except Exception as e:
        logger.error(f"Failed to connect to FTP server: {str(e)}")
        raise

# Function to get all CSV files from FTP server
def get_csv_files_from_ftp(ftp):
    """Get all CSV files from FTP server"""
    # Get all file names in the current directory
    file_list = []

    try:
        # List all files in the current directory
        ftp.retrlines('LIST', lambda x: file_list.append(x.split()[-1]))

        # Filter for .text or .csv files
        csv_files = [f for f in file_list if f.endswith('.text') or f.endswith('.csv')]

        if not csv_files:
            logger.warning("No CSV files found on FTP server")
            return []

        logger.info(f"Found {len(csv_files)} CSV files on FTP server")
        return csv_files

    except Exception as e:
        logger.error(f"Error listing files on FTP server: {str(e)}")
        return []

# Function to download and parse a CSV file from FTP
def download_and_parse_csv(ftp, filename):
    """Download and parse a CSV file from FTP server"""
    try:
        # Create a temporary file to store CSV content
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            # Download the file
            logger.info(f"Downloading {filename} from FTP server")
            ftp.retrbinary(f"RETR {filename}", temp_file.write)
            temp_filename = temp_file.name

        # Read the CSV file
        with open(temp_filename, 'r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            rows = list(reader)

        # Clean up temporary file
        os.unlink(temp_filename)

        logger.info(f"Successfully parsed {filename} with {len(rows)} rows")
        return rows

    except Exception as e:
        logger.error(f"Error downloading or parsing {filename}: {str(e)}")
        return []

# Function to get existing ISBNs from Supabase tables
def get_existing_isbns(supabase):
    """Get existing ISBNs from Inventory and Below Stock tables"""
    existing_isbns = set()

    try:
        # Query Inventory table
        inventory_response = supabase.table("Inventory").select("isbn").execute()
        for item in inventory_response.data:
            existing_isbns.add(item['isbn'])

        # Query Below Stock table
        below_stock_response = supabase.table("Below Stock").select("isbn").execute()
        for item in below_stock_response.data:
            existing_isbns.add(item['isbn'])

        logger.info(f"Found {len(existing_isbns)} existing ISBNs in Supabase tables")
        return existing_isbns

    except Exception as e:
        logger.error(f"Error retrieving existing ISBNs from Supabase: {str(e)}")
        return set()

# Function to insert new ISBNs into the "New ISBN" table
def insert_new_isbns(supabase, new_isbn_records):
    """Insert new ISBN records into the New ISBN table"""
    if not new_isbn_records:
        logger.info("No new ISBNs to insert")
        return 0

    try:
        # Insert records in batches to avoid payload size limits
        batch_size = 100
        total_inserted = 0

        for i in range(0, len(new_isbn_records), batch_size):
            batch = new_isbn_records[i:i+batch_size]
            response = supabase.table("New ISBN").insert(batch).execute()
            total_inserted += len(batch)
            logger.info(f"Inserted batch of {len(batch)} new ISBNs")

        logger.info(f"Successfully inserted {total_inserted} new ISBN records")
        return total_inserted

    except Exception as e:
        logger.error(f"Error inserting new ISBNs into Supabase: {str(e)}")
        return 0

# Main function to discover new ISBNs
def discover_new_isbns():
    """Main function to discover new ISBNs from FTP inventory"""
    logger.info("Starting new ISBN discovery process...")

    try:
        # Setup connections
        supabase = setup_supabase()
        ftp = connect_to_ftp()

        # Get existing ISBNs from Supabase
        existing_isbns = get_existing_isbns(supabase)

        # Get all CSV files from FTP server
        csv_files = get_csv_files_from_ftp(ftp)

        if not csv_files:
            logger.warning("No CSV files found. Exiting.")
            return

        # Process each CSV file
        new_isbn_records = []
        min_stock_threshold = 4  # Stock must be 4 or more

        for filename in csv_files:
            # Download and parse CSV file
            rows = download_and_parse_csv(ftp, filename)

            # Process each row
            for row in rows:
                # Skip if ISBN is missing
                if 'isbn' not in row:
                    continue

                isbn = row['isbn'].strip()

                # Skip if ISBN is already in Supabase
                if isbn in existing_isbns:
                    continue

                # Check stock level
                try:
                    stock = int(row.get('stock', 0))
                except (ValueError, TypeError):
                    stock = 0

                # Skip if stock is less than threshold
                if stock < min_stock_threshold:
                    continue

                # Get other fields
                record = {
                    'isbn': isbn,
                    'stock': stock,
                    'title': row.get('title', ''),
                    'author': row.get('author', ''),
                    'publisher': row.get('publisher', ''),
                    'rrp': float(row.get('rrp', 0)) if row.get('rrp') else None,
                    'discovered_at': datetime.now().isoformat(),
                    'source_file': filename
                }

                # Add to new ISBNs list
                new_isbn_records.append(record)

                # Add to existing ISBNs to avoid duplicates
                existing_isbns.add(isbn)

        # Insert new ISBNs into Supabase
        total_inserted = insert_new_isbns(supabase, new_isbn_records)

        # Close FTP connection
        ftp.quit()

        logger.info(f"New ISBN discovery completed! Found {len(new_isbn_records)} new ISBNs with stock >= {min_stock_threshold}")

    except Exception as e:
        logger.error(f"Error during new ISBN discovery process: {str(e)}")

# For Google Colab: Create a .env file with credentials
def setup_env_file():
    """Create a .env file with environment variables for Google Colab"""
    print("Setting up environment variables for Google Colab...")

    env_content = """
# Supabase credentials
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key

# FTP server credentials
FTP_HOST=your_ftp_host
FTP_USER=your_ftp_username
FTP_PASS=your_ftp_password
FTP_PATH=your_ftp_directory_path
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
    discover_new_isbns()