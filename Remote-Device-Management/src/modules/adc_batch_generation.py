import numpy as np
from decimal import Decimal, ROUND_HALF_UP
import time
import csv
import os
import logging
from board import SCL, SDA
from busio import I2C
import adafruit_ads1x15.ads1015 as ADS
from adafruit_ads1x15.analog_in import AnalogIn
from datetime import datetime
import pytz
import json
from dotenv import load_dotenv, set_key
from queue_server import QueueManager
import re
import threading
from collections import deque



# Load environment variables from .env file
load_dotenv()

manager = QueueManager(address=(os.getenv('QUEUE_HOST'), int(os.getenv('QUEUE_PORT'))), authkey=os.getenv('AUTH_KEY').encode())
manager.connect()

# Configure logging
log_dir = os.getenv('LOG_DIRECTORY')
log_file = os.getenv('LOG_FILE_ADC')
os.makedirs(log_dir, exist_ok=True)
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
log_level = getattr(logging, log_level, logging.INFO)

logger = logging.getLogger(os.getenv('LOGGER_ADC'))
logger.setLevel(log_level)
file_handler_adc = logging.FileHandler(os.path.join(log_dir, log_file))
file_handler_adc.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(file_handler_adc)
samples_lcd=[]
def get_last_batch_number(backup_adc_batches):
    batch_files=[f for f in os.listdir(backup_adc_batches) if re.match(r"BFA1_Batch(\d+)_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.csv", f)]
    batch_numbers = [int(re.search(r"BFA1_Batch(\d+)", file).group(1)) for file in batch_files]
    return max(batch_numbers) if batch_numbers else 0


try:
    i2c = I2C(SCL, SDA)

    # Initialize ADC (ADS1015)
    logger.debug("Initializing ADS1015...")
    ads = ADS.ADS1015(i2c, data_rate=3300)
    chan = AnalogIn(ads, ADS.P1)
    logger.info("ADS1015 initialized successfully.")
    


    
   

    # Batch and sampling setup
    backup_adc_batches=os.getenv('FILE_DIRECTORY_ADC_BATCHES_BACKUP')
    output_folder = os.getenv('FILE_DIRECTORY_ADC_BATCHES')
    batch_number = get_last_batch_number(output_folder) + 1
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        logger.info(f"Output folder created: {output_folder}")

    batch_size = int(os.getenv('BATCH_SIZE'))
    sampling_interval = float(os.getenv('SAMPLING_RATE'))
    ist_tz = pytz.timezone(os.getenv('TIMEZONE'))
   

    # Main data collection loop
    while True:
        try:
            logger.info(f"Starting collection for batch {batch_number}.")
            samples = []
            start_time = time.time()
            
            for i in range(batch_size):
                current_time = time.time()
                formatted_timestamp = datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
                voltage = "{:.6f}".format(Decimal(str(chan.voltage)).quantize(Decimal('0.000000'),rounding=ROUND_HALF_UP))
                # voltage2 = chan.voltage
                # resistor_value = int(os.getenv('RESISTOR_VALUE'))
                # pressure_kg_cm2 = voltage_to_pressure(voltage2,resistor_value)
                samples.append([formatted_timestamp, voltage])
                # # Add voltage to moving average queue
                # moving_avg_queue.append(voltage)
                # samples_lcd.append(pressure_kg_cm2)

                time.sleep(max(0, sampling_interval - (time.time() - current_time)))

            # Save batch to CSV
            ist_timestamp = datetime.now(ist_tz).strftime('%Y-%m-%d_%H-%M-%S')
            batch_file = os.path.join(output_folder, f"BFA1_Batch{batch_number}_{ist_timestamp}.csv")

            with open(batch_file, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["Timestamp", "Voltage"])
                writer.writerows(samples)
            logger.info(f"Batch {batch_number} saved: {batch_file}")

            # Update .env with Device ID
            match = re.search(r"BFA\d+", os.path.basename(batch_file))
            if match:
                device_name = match.group(0)
                env_file_path = '/home/sulphuricrog/iptran_datalogger/.env'
                set_key(env_file_path, 'DEVICE_ID', device_name)
                logger.info(f"Updated DEVICE_ID in .env file to {device_name}")

            # Publish event to queue
            event_message = {
                "file_path": batch_file,
                "event_type": os.getenv('ADC_BATCH_CREATED_EVENT'),
            }
            manager.s3_events().put(json.dumps(event_message))
            logger.info(f"Event published to queue for batch {batch_number}.")

            batch_number += 1

        except Exception as e:
            logger.error(f"An error occurred during data collection: {e}", exc_info=True)
            time.sleep(5)  # Retry after a delay

except Exception as e:
    logger.error(f"Initialization failed: {e}")