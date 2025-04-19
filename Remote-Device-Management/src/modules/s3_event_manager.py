import os
import sys
import json
import subprocess
from queue_server import QueueManager
from dotenv import load_dotenv
import time
import boto3
import logging

# Load environment variables
load_dotenv()

# Define the folder paths and configurations
adc_batches_folder = os.getenv('FILE_DIRECTORY_ADC_BATCHES')


# Connect to the Queue Manager
manager = QueueManager(address=(os.getenv('QUEUE_HOST'), int(os.getenv('QUEUE_PORT'))), authkey=os.getenv('AUTH_KEY').encode())
manager.connect()

# Get the queues
s3_events_queue = manager.s3_events()

# AWS S3 Configuration
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')          # Replace with your AWS Access Key ID
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')  # Replace with your AWS Secret Access Key
aws_region_name = os.getenv('AWS_REGION')                  # Replace with your AWS Region
s3_bucket_name = os.getenv('S3_BUCKET_NAME')            # Replace with your S3 bucket name
dynamodb_table = os.getenv('DYNAMODB_TABLE')
# Configure logging
log_dir = os.getenv('LOG_DIRECTORY')  # Replace with your desired directory
log_file = os.getenv('LOG_FILE_FILE_WATCHER')
os.makedirs(log_dir, exist_ok=True)
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()  # Default to INFO if not set
log_level = getattr(logging, log_level, logging.INFO)  # Default to logging.INFO if invalid
try:
    logger = logging.getLogger(os.getenv('LOGGER_FILE_WATCHER'))
    logger.setLevel(log_level)  # Set the log 

    file_handler_s3_manager = logging.FileHandler(os.path.join(log_dir, log_file))
    file_handler_s3_manager.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S"))
    
    # Add handlers to the ADC logger
    logger.addHandler(file_handler_s3_manager)
except Exception as e:
    logger.error(f"Logging setup failed: {e}")

# Ensure directories exist
for folder in [adc_batches_folder]:
    if not os.path.exists(folder):
        os.makedirs(folder)

s3 = boto3.client('s3',
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  region_name=aws_region_name)


# Python interpreter for script execution
python_interpreter = os.getenv('PYTHON_INTERPRETER')

# Downsampling and NPW classification script paths

s3_upload_script = os.path.join(os.getenv('BASE_DIRECTORY'),'src/modules','s3_upload.py')
# dynamodb_upload_script = os.path.join(os.getenv('BASE_DIRECTORY'),'src/modules','dynamodb_upload.py')


#Function for uploading to S3 
def upload_to_s3(file_path):
    file_name = os.path.basename(file_path)
    try:
        subprocess.run([python_interpreter, s3_upload_script, file_path], check=True)
        logger.info(f"Uploaded {file_name} to S3 bucket: {s3_bucket_name}")
        return True
    except Exception as e:
        logger.error(f"Error uploading {file_name} to S3: {e}")
        return False

def upload_to_dynamodb(file_path):
    file_name = os.path.basename(file_path)
    try:
        subprocess.run([python_interpreter, dynamodb_upload_script, file_path], check=True)
        logger.info(f"Uploaded {file_name} to S3 bucket: {dynamodb_table}")
        return True
    except Exception as e:
        logger.error(f"Error uploading {file_name} to S3: {e}")
        return False


# Function to process the received event data
def process_event(event_data):
    # Extract the details from the event data
    file_path = event_data.get("file_path")
    logger.debug(file_path)
    event_type = event_data.get("event_type")
    logger.debug(event_type)
    

    # Validate event data 
    if not file_path or not event_type:
        logger.error(f"Invalid event data received: {event_data}")
        return

    # Process based on event type


    if event_type == os.getenv('ADC_BATCH_CREATED_EVENT'):
        logger.info(f"Processing for S3 Upload: {file_path}")
        upload_to_s3(file_path)
    else:
        logger.error(f"Unknown event type: {event_type}")

# Main entry point
if __name__ == "__main__":
    
    while True:
        try:
            # Listen for events
            if not s3_events_queue.empty():
                message = s3_events_queue.get()            
    
                logger.info(f"Event received from s3_events_queue': {message}")

                # Parse the JSON message
                event_data = json.loads(message)

                #Process the event
                process_event(event_data)

        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON data: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Event encountered an error: {e}")


