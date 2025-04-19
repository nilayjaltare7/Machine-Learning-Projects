from multiprocessing.managers import BaseManager
from queue import Queue
import os
from dotenv import load_dotenv
import logging
# Load environment variables from .env file
load_dotenv()
 

# Logging Config
log_dir = os.getenv('LOG_DIRECTORY')  # Replace with your desired directory
log_file = os.getenv('LOG_FILE_QUEUE_SERVER')
os.makedirs(log_dir, exist_ok=True)
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()  # Default to INFO if not set
log_level = getattr(logging, log_level, logging.INFO)  # Default to logging.INFO if invalid
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, log_file)),  # Logs to file
        logging.StreamHandler()  # Logs to console
    ],
)

# Define queues for the event system
file_events_queue = Queue()
iot_events_queue = Queue()
s3_events_queue = Queue()

class QueueManager(BaseManager):
    pass

# Register the queues with the manager
QueueManager.register(os.getenv('FILE_EVENTS_ID'), callable=lambda: file_events_queue)
QueueManager.register(os.getenv('IOT_EVENTS_ID'), callable=lambda: iot_events_queue)
QueueManager.register(os.getenv('S3_EVENTS_ID'), callable=lambda: s3_events_queue)

if __name__ == "__main__":
    manager = QueueManager(address=(os.getenv('QUEUE_HOST'), int(os.getenv('QUEUE_PORT'))), authkey=os.getenv('AUTH_KEY').encode())
    server = manager.get_server()
    logging.info("QueueManager server started.")
    server.serve_forever()
