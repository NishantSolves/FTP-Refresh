# First install the required packages
!pip install python-supabase python-dotenv ftplib

import os
import csv
import io
import tempfile
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
import logging
import ftplib

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("inventory_sync.log")
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

# Define the inventory table name in Supabase
INVENTORY_TABLE = "inventory"
TEMP_MODIFIED_TABLE = f"modified_inventory_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def create_temp_table(supabase: Client):
    """Create a temporary table to store modified records"""
    logger.info(f"Creating temporary table: {TEMP_MODIFIED_TABLE}")

    # SQL query to create the table based on the existing inventory table
    query = f"""
    CREATE TABLE {TEMP_MODIFIED_TABLE} AS
    SELECT isbn, stock as old_stock, stock as new_stock, rrp as old_rrp, rrp as new_rrp,
           CURRENT_TIMESTAMP as modified_at
    FROM {INVENTORY_TABLE}
    WHERE 1=0;
    """

    # Execute raw SQL query
    supabase.table("dummy").select("*").limit(1).execute()  # Ensure connection is working

    # Using RPC call to execute raw SQL
    supabase.rpc('exec_sql', {'sql': query}).execute()

    logger.info(f"Temporary table {TEMP_MODIFIED_TABLE} created successfully")

def get_inventory_from_supabase(supabase: Client):
    """Retrieve current inventory data from Supabase"""
    response = supabase.table(INVENTORY_TABLE).select("isbn, stock, rrp").execute()

    # Convert to dictionary with ISBN as key for easier lookup
    inventory_dict = {}
    for item in response.data:
        inventory_dict[item['isbn']] = {
            'stock': item['stock'],
            'rrp': item['rrp']
        }

    logger.info(f"Retrieved {len(inventory_dict)} inventory items from Supabase")
    return inventory_dict

def process_csv_files(supabase: Client, ftp):
    """Process all CSV files from FTP server and compare with Supabase inventory"""
    # Get all CSV files from FTP server
    csv_files = get_csv_files_from_ftp(ftp)

    if not csv_files:
        logger.warning("No CSV files found on FTP server. Exiting.")
        return []

    # Get current inventory from Supabase
    supabase_inventory = get_inventory_from_supabase(supabase)

    # Track modified records
    modified_records = []

    # Process each CSV file
    for filename in csv_files:
        logger.info(f"Processing {filename}...")

        # Download and parse CSV file
        rows = download_and_parse_csv(ftp, filename)

        # Process each row
        for row in rows:
            isbn = row.get('isbn')

            if not isbn:
                logger.warning(f"Warning: Row missing ISBN in {filename}")
                continue

            # Get the current CSV values
            try:
                csv_stock = int(row.get('stock', 0))
                csv_rrp = float(row.get('rrp', 0.0))
            except (ValueError, TypeError):
                logger.warning(f"Warning: Invalid stock or RRP value for ISBN {isbn}")
                continue

            # Check if this ISBN exists in Supabase
            if isbn in supabase_inventory:
                supabase_stock = supabase_inventory[isbn]['stock']
                supabase_rrp = supabase_inventory[isbn]['rrp']

                # Check for differences
                if csv_stock != supabase_stock or csv_rrp != supabase_rrp:
                    logger.info(f"Difference found for ISBN {isbn}:")
                    logger.info(f"  Stock: {supabase_stock} → {csv_stock}")
                    logger.info(f"  RRP: {supabase_rrp} → {csv_rrp}")

                    # Update record in Supabase
                    supabase.table(INVENTORY_TABLE) \
                        .update({"stock": csv_stock, "rrp": csv_rrp}) \
                        .eq("isbn", isbn) \
                        .execute()

                    # Store for the temporary table
                    modified_records.append({
                        "isbn": isbn,
                        "old_stock": supabase_stock,
                        "new_stock": csv_stock,
                        "old_rrp": supabase_rrp,
                        "new_rrp": csv_rrp,
                        "modified_at": datetime.now().isoformat()
                    })
            else:
                logger.info(f"ISBN {isbn} not found in Supabase inventory")

    logger.info(f"Found {len(modified_records)} records with differences")
    return modified_records

def save_modified_records(supabase: Client, modified_records):
    """Save modified records to the temporary table"""
    if not modified_records:
        logger.info("No records were modified")
        return

    logger.info(f"Saving {len(modified_records)} modified records to {TEMP_MODIFIED_TABLE}")

    # Insert records into temporary table
    supabase.table(TEMP_MODIFIED_TABLE).insert(modified_records).execute()

    logger.info("Modified records saved successfully")

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

def main():
    """Main function to run the inventory synchronization process"""
    logger.info("Starting inventory synchronization process...")

    try:
        # For Colab, create an env file template if needed
        if 'google.colab' in str(get_ipython()):
            setup_env_file()

        # Setup connections
        supabase = setup_supabase()
        ftp = connect_to_ftp()

        # Create temporary table for tracking modifications
        create_temp_table(supabase)

        # Process CSV files and get modified records
        modified_records = process_csv_files(supabase, ftp)

        # Save modified records to temporary table
        save_modified_records(supabase, modified_records)

        # Close FTP connection
        ftp.quit()

        logger.info(f"Inventory synchronization completed! Modified records: {len(modified_records)}")

    except Exception as e:
        logger.error(f"Error during synchronization process: {str(e)}")

if __name__ == "__main__":
    main()