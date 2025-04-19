import boto3
from dotenv import load_dotenv
import sys
import os
import schedule
import time
import logging
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, BotoCoreError, ClientError
import re

# Load environment variables from .env file
load_dotenv()

# Configure logging
log_dir = os.getenv('LOG_DIRECTORY')  # Replace with your desired directory
log_file = os.getenv('LOG_FILE_S3_UPLOAD')
os.makedirs(log_dir, exist_ok=True)
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()  # Default to INFO if not set
log_level = getattr(logging, log_level, logging.INFO)  # Default to logging.INFO if invalid
try:
    logger = logging.getLogger(os.getenv('LOGGER_S3_UPLOAD'))
    logger.setLevel(log_level)  # Set the log level

    file_handler_s3 = logging.FileHandler(os.path.join(log_dir, log_file))
    file_handler_s3.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S"))
    
    # Add handlers to the ADC logger
    logger.addHandler(file_handler_s3)
except Exception as e:
    logger.error(f"Logging setup failed: {e}")

# AWS S3 Configuration
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region_name = os.getenv('AWS_REGION')
s3_bucket_name = os.getenv('S3_BUCKET_NAME')

backup_folder = os.getenv('FILE_DIRECTORY_LEAKED_BATCHES_BACKUP')
directory = os.getenv("FILE_DIRECTORY_ADC_BATCHES_BACKUP")
# os.makedirs(backup_folder, exist_ok=True)

# Create an S3 client using your credentials
try:
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region_name
    )
    logger.info("S3 client initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize S3 client: {e}")
    sys.exit(1)

def create_folders_in_s3(s3_bucket_name, folder_names):
    """
    Creates multiple folders in the specified S3 bucket.

    Args:
        s3_bucket_name (str): Name of the S3 bucket.
        folder_names (list): List of folder names to create.
    """
    try:
        # Create each folder
        for folder_name in folder_names:
            if not folder_name.endswith('/'):
                folder_name += '/'
            s3.put_object(Bucket=s3_bucket_name, Key=folder_name)
            logger.info(f"Folder '{folder_name}' created successfully in bucket '{s3_bucket_name}'.")
    except NoCredentialsError:
        logger.error("AWS credentials not found. Please configure them.")
    except PartialCredentialsError:
        logger.error("Incomplete AWS credentials. Please check your configuration.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

s3_bucket_name = os.getenv('S3_BUCKET_NAME')
folder_names = ["BFA1", "BFA2", "BFA3"]

create_folders_in_s3(s3_bucket_name, folder_names)

def upload_to_s3(leaked_file_path):
    """
    Uploads a file to the specified S3 bucket with the folder name containing the timestamp.
    """
    if isinstance(leaked_file_path, str):
        leaked_file_path = [leaked_file_path]  # Convert single file path to list

    for file_path in leaked_file_path:
        file_name = os.path.basename(file_path)
        logger.info(f"Processing file: {file_name}")

        # Extracting BFAx and Timestamp
        match = re.search(r"(BFA\d+)_(Batch\d+)_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})", file_name)
        if match:
            batch_number = match.group(2)
            main_folder = match.group(1)  # Extract 'BFAx'
            timestamp = match.group(3)  # Extract 'YYYY-MM-DD_HH-MM-SS'
            folder_path = f"{main_folder}/{file_name}"  # Folder structure: BFAx/YYYY-MM-DD_HH-MM-SS
        else:
            folder_path = "Unclassified"
            logger.warning(f"File {file_name} uploaded to Unclassified folder")
            batch_number = None

        # Set full path in S3
        s3_key = folder_path

        try:
            s3.upload_file(file_path, s3_bucket_name, s3_key)
            logger.info(f"Uploaded {file_path} to S3: {s3_bucket_name}/{s3_key}")
            # move_to_backup(file_path)  # Move to backup after successful upload
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error uploading {file_path} to S3: {e}")
            return False

    return True



# Check if a filename argument is passed
if len(sys.argv) == 2:
    save_path = sys.argv[1]

    if not os.path.isfile(save_path):
        logger.error(f"The file {save_path} does not exist.")
        sys.exit(1)

    # Upload the specified file
    upload_to_s3(save_path)
    sys.exit(0)  # Exit after processing the file

else:
    while True:
        schedule.run_pending()
        time.sleep(1)