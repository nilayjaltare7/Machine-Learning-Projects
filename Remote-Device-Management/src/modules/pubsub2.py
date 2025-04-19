# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import sys
import os
import re

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import threading
import time
import json
import subprocess
from queue_server import QueueManager
#from utils.command_line_utils import CommandLineUtils
from dotenv import load_dotenv



manager = QueueManager(address=(os.getenv('QUEUE_HOST'), int(os.getenv('QUEUE_PORT'))), authkey=os.getenv('AUTH_KEY').encode())
manager.connect()

# Get the queues
iot_events_queue = manager.iot_events()

load_dotenv()

# Configure logging
log_dir = os.getenv('LOG_DIRECTORY')  # Replace with your desired directory
log_file = os.getenv('LOG_FILE_IOT')
os.makedirs(log_dir, exist_ok=True)
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()  # Default to INFO if not set
log_level = getattr(logging, log_level, logging.INFO)  # Default to logger.INFO if invalid
#id_serial_number = {"1420224231781" : 'BFA-2', "1420224232263" : 'BFA-3', "1420224231942" : 'BFA-1'}
try:
    logger = logging.getLogger(os.getenv('LOGGER_IOT'))
    logger.setLevel(log_level)  # Set the log 

    file_handler_file_watcher = logging.FileHandler(os.path.join(log_dir, log_file))
    file_handler_file_watcher.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S"))
    
    # Add handlers to the ADC logger
    logger.addHandler(file_handler_file_watcher)
except Exception as e:
    logger.error(f"Logging setup failed: {e}")

# Get the serial number of the Jetson Nano
try:
    serial_number = subprocess.check_output("cat /proc/device-tree/serial-number", shell=True).decode().strip().replace("\u0000", "")
    logger.info(f"Jetson Nano Serial Number: {serial_number}")
except Exception as e:
    logger.error("Failed to retrieve serial number", exc_info=True)
    sys.exit(1)

lwt_payload = {
  "state": {
    "reported": {
      "CONFIG": {
        "STATUS": "OFF"
      }
    }
  }
}

active_payload = {
  "state": {
    "reported": {
      "CONFIG": {
        "STATUS": "ACTIVE"
      }
    }
  }
}

serial_number_payload = {
    "state" : {
        "reported" : {
            "ACCOUNT" : {
                "SERIAL_NO" : serial_number,
                "DEVICE_NAME" : id_serial_number[serial_number]
            }
        }

    }
}

message_topic = os.getenv('IOT_UPDATE_TOPIC')
message_topic = message_topic.replace("$$macid", serial_number)
logger.info(message_topic)

lwt_topic = os.getenv('IOT_LWT_TOPIC')
lwt_topic = lwt_topic.replace("$$macid", serial_number)
logger.info(lwt_topic)



willfunction=mqtt.Will(
        topic=lwt_topic,
        payload=json.dumps(lwt_payload).encode('utf-8'),
        qos=mqtt.QoS.AT_LEAST_ONCE,
        retain=False
        )

# This sample uses the Message Broker for AWS IoT to send and receive messages
# through an MQTT connection. On startup, the device connects to the server,
# subscribes to a topic, and begins publishing messages to that topic.
# The device should receive those same messages back from the message broker,
# since it is subscribed to that same topic.

# cmdData is the arguments/input from the command line placed into a single struct for
# use in this sample. This handles all of the command line parsing, validating, etc.
# See the Utils/CommandLineUtils for more information.
#cmdData = CommandLineUtils.parse_sample_input_pubsub()

received_count = 0
received_all_event = threading.Event()


# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    logger.error("Connection interrupted. error: {}".format(error))


# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    logger.info("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        logger.error("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()


        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)
    if return_code == mqtt.ConnectReturnCode.ACCEPTED:
      mqtt_connection.publish(
                topic=message_topic,
                payload=json.dumps(active_payload).encode('utf-8'),
                qos=mqtt.QoS.AT_LEAST_ONCE)
      


def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    logger.info("Resubscribe results: {}".format(resubscribe_results))

    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            logger.error("Server rejected resubscribe to topic: {}".format(topic))
            sys.exit("Server rejected resubscribe to topic: {}".format(topic))


# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    logger.info("Received message from topic '{}': {}".format(topic, payload))
    global received_count
    received_count += 1
    # if received_count == os.getenv('COUNT'):
        # received_all_event.set()

# Callback when the connection successfully connects
def on_connection_success(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
    mqtt_connection.publish(
                topic=message_topic,
                payload=json.dumps(active_payload).encode('utf-8'),
                qos=mqtt.QoS.AT_LEAST_ONCE)
    logger.info("Connection Successful with return code: {} session present: {}".format(callback_data.return_code, callback_data.session_present))

# Callback when a connection attempt fails
def on_connection_failure(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionFailureData)
    logger.error("Connection failed with error code: {}".format(callback_data.error))

# Callback when a connection has been disconnected or shutdown successfully
def on_connection_closed(connection, callback_data):
    logger.info("Connection closed")

if __name__ == '__main__':
    # Create the proxy options if the data is present in cmdData
    proxy_options = None
    # if cmdData.input_proxy_host is not None and cmdData.input_proxy_port != 0:
    #     proxy_options = http.HttpProxyOptions(
    #         host_name=cmdData.input_proxy_host,
    #         port=cmdData.input_proxy_port)
    jetson_cert_path= os.path.join(os.getenv('BASE_DIRECTORY'),'src/modules/iot','jetson.cert.crt')
    pri_key_path= os.path.join(os.getenv('BASE_DIRECTORY'),'src/modules/iot','jetson.private.key')
    root_ca_path= os.path.join(os.getenv('BASE_DIRECTORY'),'src/modules/iot','root-CA.crt')



    # Create a MQTT connection from the command line data
    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=os.getenv('IOT_ENDPOINT'),
        # port=8883,
        cert_filepath=jetson_cert_path,
        pri_key_filepath=pri_key_path,
        ca_filepath=root_ca_path,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        client_id=serial_number,
        clean_session=False,
        keep_alive_secs=5,
        http_proxy_options=proxy_options,
        on_connection_success=on_connection_success,
        on_connection_failure=on_connection_failure,
        on_connection_closed=on_connection_closed,
        will=willfunction
    )

    # if not cmdData.input_is_ci:
    #     logger.debug(f"Connecting to {os.getenv('IOT_ENDPOINT')} with client ID '{os.getenv('IOT_CLIENT_ID')}'...")
    # else:
    #     logger.info("Connecting to endpoint with client ID")
    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    logger.info("Connected")

    message_count = int(os.getenv('COUNT'))
    
    logger.info(message_topic)
    message_string = 'Hello World'

    # Subscribe
    logger.debug("Subscribing to topic '{}'...".format(message_topic))
    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic=message_topic,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received)

    subscribe_result = subscribe_future.result()
    logger.info("Subscribed with {}".format(str(subscribe_result['qos'])))

    while True:
      try:
          if not iot_events_queue.empty():
              message = iot_events_queue.get()

              # Parse the JSON message
              event_data = json.loads(message)
              event_type = event_data.get("event_type")
              
              
              

              if(event_type=='GPS_UPDATE'):
                last_reported_time = event_data.get("time")
                latitude = event_data.get("latitude")
                longitude = event_data.get("longitude")
                maps_link = event_data.get("maps_link")
                gps_payload = {
                    "state": {
                        "reported": {
                            "GPS": {
                                "LAST_REPORTED": last_reported_time,  # Replace $$time with current time
                                "LATITUDE": latitude,           # Replace $$latitude with actual latitude
                                "LONGITUDE": longitude,         # Replace $$longitude with actual longitude
                                "MAPS_LINK": maps_link         # Replace $$mapsLink with generated maps link
                            }
                        }
                    }
                }
                mqtt_connection.publish(
                topic=message_topic,
                payload=json.dumps(gps_payload).encode('utf-8'),
                qos=mqtt.QoS.AT_LEAST_ONCE)
              elif(event_type=='MONITORING'):
                status = event_data.get("LEAK")
                classified_batch_dir = event_data.get("file_path")
                classification = event_data.get("classification")
                classified_batch = os.path.basename(classified_batch_dir)
                pattern = r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})"
                match = re.search(pattern,classified_batch)

                if classification == 'LEAKED':
                    date_time = match.group(1)
                else:
                    date_time = "NO LEAK"


                status_payload = {
                    "state": {
                        "reported": {
                            "MONITORING": {
                                "LEAK": status,  # Replace $$time with current time
                                "LAST_LEAK_REPORTED_TIME" : date_time

                            }
                        }
                    }
                }
                mqtt_connection.publish(
                topic=message_topic,
                payload=json.dumps(status_payload).encode('utf-8'),
                qos=mqtt.QoS.AT_LEAST_ONCE)
                mqtt_connection.publish(
                topic=message_topic,
                payload=json.dumps(serial_number_payload).encode('utf-8'),
                qos=mqtt.QoS.AT_LEAST_ONCE)

        
              # Validate message content
              if not data or not event_type:
                  logger.error(f"Invalid message format: {message}")
                  continue
          else:
              continue

      except Exception as e:
          logger.error(f"Subscriber encountered an error: {e}")

    # Publish message to server desired number of times.
    # This step is skipped if message is blank.
    # This step loops forever if count was set to 0.
    if message_string:
        if message_count == 0:
            logger.info("Sending messages until program killed")
        else:
            logger.info("Sending {} message(s)".format(message_count))

        publish_count = 1
        # while (publish_count <= message_count) or (message_count == 0):
        #     message = "{} [{}]".format(message_string, publish_count)
        #     logger.debug("Publishing message to topic '{}': {}".format(message_topic, message))
        #     message_json = json.dumps(message)
        #     mqtt_connection.publish(
        #         topic=message_topic,
        #         payload=message_json,
        #         qos=mqtt.QoS.AT_LEAST_ONCE)
        #     time.sleep(1)
        #     publish_count += 1

    # Wait for all messages to be received.
    # This waits forever if count was set to 0.
    if message_count != 0 and not received_all_event.is_set():
        logger.debug("Waiting for all messages to be received...")

    received_all_event.wait()
    logger.debug("Waiting for all messages to be received...")

    # Disconnect
    logger.info("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    logger.info("Disconnected!")