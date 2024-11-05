import asyncio
import json
import logging
import signal

from vehicle import Vehicle, vehicle  # type: ignore
from velocitas_sdk.util.log import get_opentelemetry_log_factory, get_opentelemetry_log_format  # type: ignore
from velocitas_sdk.vdb.reply import DataPointReply
from velocitas_sdk.vehicle_app import VehicleApp, subscribe_topic

# Configure logger with OpenTelemetry
logging.setLogRecordFactory(get_opentelemetry_log_factory())
logging.basicConfig(format=get_opentelemetry_log_format())
logging.getLogger().setLevel("INFO")
logger = logging.getLogger(__name__)

# MQTT topic configuration
VEHICLE_SPEED_TOPIC = "vehicle/speed"

class SpeedSubscriberApp(VehicleApp):
    """
    SpeedSubscriberApp listens for speed data on the MQTT topic "vehicle/speed"
    and writes it to the Vehicle Model.
    """

    def __init__(self, vehicle_client: Vehicle):
        super().__init__()
        self.vehicle = vehicle_client

    async def on_start(self):
        """This method is called when the app starts."""
        logger.info("SpeedSubscriberApp started and waiting for speed updates on MQTT topic...")

    @subscribe_topic(VEHICLE_SPEED_TOPIC)
    async def on_speed_received(self, data_str: str) -> None:
        """
        Callback for handling incoming speed data from the MQTT topic.
        """
        try:
            # Parse the incoming speed data
            speed = int(data_str)

            if speed is not None:
                # Write the speed to the Vehicle Model
                await self.vehicle.Speed.set(speed)
                logger.info(f"Received speed: {speed} km/h and wrote it to Vehicle Model.")
            else:
                logger.warning("Received speed data is missing the 'speed' field.")
        
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON from received MQTT message.")

async def main():
    """Main entry point for the Vehicle App."""
    logger.info("Starting SpeedSubscriberApp...")
    vehicle_app = SpeedSubscriberApp(vehicle)
    await vehicle_app.run()

if __name__ == "__main__":
    asyncio.run(main())
