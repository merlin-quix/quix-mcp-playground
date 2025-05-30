import os
import time
from datetime import datetime
from quixstreams import Application

# Set environment variables
# For deployed applications, these will be set via the Quix platform
# For local development, you can set them here or in a .env file
if "Quix__Sdk__Token" not in os.environ:
    os.environ["Quix__Sdk__Token"] = "sdk-8310f429a0b34c03b7f684d8409502bf"

# Use environment variable for input topic, fallback to cnc-data
input_topic_name = os.environ.get("input", "cnc-data")

def create_consumer_application():
    """
    Create and configure the Quix Streams consumer application
    """
    # Create a unique consumer group based on current time to avoid conflicts
    consumer_group = f"cnc-consumer-{int(time.time())}"
    
    print(f"Creating Quix Streams consumer application...")
    print(f"Consumer Group: {consumer_group}")
    print(f"Topic: {input_topic_name}")
    print(f"Will process messages with stop condition\n")
    
    # Create the Application
    app = Application(
        broker_address=None,  # Quix Cloud doesn't need broker address
        consumer_group=consumer_group,
        auto_offset_reset="earliest"  # Start from the beginning
    )
    
    return app

def process_cnc_data():
    """
    Main function to process CNC data from Kafka topic
    """
    app = create_consumer_application()
    
    # Define the input topic
    input_topic = app.topic(input_topic_name)
    
    # Create a streaming dataframe
    sdf = app.dataframe(input_topic)
    
    # Message counter for stopping condition
    message_count = 0
    max_messages = 100  # Stop after 100 messages for safety
    
    def process_message(message):
        nonlocal message_count
        message_count += 1
        
        # Extract key information from CNC data
        program_name = message.get('programName', 'Unknown')
        spindle_speed = message.get('spindleActualSpeed', 0)
        spindle_temp = message.get('spindleTemp', 0)
        active_tool = message.get('activeToolID', 'Unknown')
        z_axis_power = message.get('axisZDrivePower', 0)
        timestamp_s = message.get('timestamp_s', 0)
        
        print(f"Message {message_count}:")
        print(f"  Program: {program_name}")
        print(f"  Spindle Speed: {spindle_speed} RPM")
        print(f"  Spindle Temp: {spindle_temp}°C")
        print(f"  Active Tool: {active_tool}")
        print(f"  Z-Axis Power: {z_axis_power}")
        print(f"  Timestamp: {timestamp_s}s")
        print("-" * 50)
        
        # Stop after reaching max messages to prevent infinite consumption
        if message_count >= max_messages:
            print(f"Reached {max_messages} messages. Stopping for safety...")
            app.stop()
        
        return message
    
    # Apply the processing function
    sdf = sdf.apply(process_message)
    
    print("Starting CNC data consumer...")
    print("=" * 60)
    
    try:
        # Run the application
        app.run()
        print(f"\nConsumer stopped after processing {message_count} messages.")
    except KeyboardInterrupt:
        print(f"\nConsumer interrupted by user after {message_count} messages.")
    except Exception as e:
        print(f"\nError occurred: {e}")
        print(f"Processed {message_count} messages before error.")

def main():
    """
    Main entry point for the CNC data consumer application
    """
    print("CNC Data Consumer Application")
    print("============================")
    print(f"Input Topic: {input_topic_name}")
    print(f"SDK Token: {'***' + os.environ.get('Quix__Sdk__Token', '')[-4:] if os.environ.get('Quix__Sdk__Token') else 'Not set'}")
    print()
    
    try:
        process_cnc_data()
    except Exception as e:
        print(f"Application error: {e}")
        raise

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting due to user interrupt.")
    except Exception as e:
        print(f"Fatal error: {e}")
