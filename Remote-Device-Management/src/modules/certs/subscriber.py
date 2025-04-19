from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import subprocess
import signal
import os
import platform

# Configuration
CLIENT_ID = "RaspberryPiClient"
ENDPOINT = "a2f2c1nwcciyoh-ats.iot.us-east-1.amazonaws.com"  
TOPIC = "test/topic"  

# Paths to Certificates & Keys
ROOT_CA = "/home/sulphuricrog/iptran_datalogger/src/modules/certs/AmazonRootCA1.pem"  
CERTIFICATE = "/home/sulphuricrog/iptran_datalogger/src/modules/certs/f3fc6a19234197be8f9aaf5984c06755fe61239615e1f97b7b47f03a47aee49e-certificate.pem.crt"
PRIVATE_KEY = "/home/sulphuricrog/iptran_datalogger/src/modules/certs/f3fc6a19234197be8f9aaf5984c06755fe61239615e1f97b7b47f03a47aee49e-private.pem.key"

# Local script to execute
LOCAL_SCRIPT = "/home/sulphuricrog/iptran_datalogger/src/modules/adc_batch_generation.py"

# Initialize MQTT Client
mqtt_client = AWSIoTMQTTClient(CLIENT_ID)
mqtt_client.configureEndpoint(ENDPOINT, 8883)
mqtt_client.configureCredentials(ROOT_CA, PRIVATE_KEY, CERTIFICATE)

# MQTT Configuration
mqtt_client.configureAutoReconnectBackoffTime(1, 32, 20)
mqtt_client.configureOfflinePublishQueueing(-1)  
mqtt_client.configureDrainingFrequency(2)  
mqtt_client.configureConnectDisconnectTimeout(10)  
mqtt_client.configureMQTTOperationTimeout(5)  

# Global variable to track running process
running_process = None  

def start_process():
    """Starts the local script process if not already running."""
    global running_process

    if running_process is None:
        print("Starting local script...")
        try:
            running_process = subprocess.Popen(
                ["python3", LOCAL_SCRIPT], 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, 
                text=True
            )
            print(f"Script started with PID {running_process.pid}")
        except Exception as e:
            print(f"Failed to start script: {e}")
    else:
        print("Script is already running.")


def stop_process():
    """Stops the running process if it's active."""
    global running_process

    if running_process is not None:
        print(f"Stopping script with PID {running_process.pid}...")
        try:
            if platform.system() == "Windows":
                running_process.terminate()  # Safe termination on Windows
            else:
                os.kill(running_process.pid, signal.SIGTERM)  # Graceful stop for Linux/macOS

            running_process.wait()  # Ensure complete process termination
            running_process = None
            print("Script stopped successfully.")

        except Exception as e:
            print(f"Error stopping script: {e}")
    else:
        print("No script is currently running.")

# Callback Function for MQTT Messages
def message_callback(client, userdata, message):
    payload = message.payload.decode()
    print(f"Received message on topic {message.topic}: {payload}")

    try:
        data = json.loads(payload)
        if isinstance(data, dict) and "command" in data:
            command = data["command"].lower()
            
            if command == "start":
                start_process()
            elif command == "stop":
                stop_process()
            else:
                print("Invalid command received.")

    except json.JSONDecodeError:
        print("Invalid JSON format received.")

# Connect to AWS IoT and Subscribe
try:
    print("Connecting to AWS IoT...")
    mqtt_client.connect()
    print(f"Connected! Subscribing to topic: {TOPIC}")
    
    mqtt_client.subscribe(TOPIC, 1, message_callback)

    print(f"Subscribed to {TOPIC}, waiting for messages...")

    while True:
        time.sleep(1)

except Exception as e:
    print(f"Error: {e}")

finally:
    mqtt_client.disconnect()
