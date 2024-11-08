# Copyright (c) 2024 Contributors to the Eclipse Foundation
#
# This program and the accompanying materials are made available under the
# terms of the Apache License, Version 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# SPDX-License-Identifier: Apache-2.0

import asyncio
import base64
import json
import logging
import os

import firebase_admin  # type: ignore
from firebase_admin import credentials, firestore
from vehicle import Vehicle, vehicle  # type: ignore
from velocitas_sdk.util.log import (  # type: ignore
    get_opentelemetry_log_factory,
    get_opentelemetry_log_format,
)
from velocitas_sdk.vdb.reply import DataPointReply
from velocitas_sdk.vehicle_app import VehicleApp

# Configure logger with OpenTelemetry
logging.setLogRecordFactory(get_opentelemetry_log_factory())
logging.basicConfig(format=get_opentelemetry_log_format())
logging.getLogger().setLevel("INFO")
logger = logging.getLogger(__name__)

# MQTT topic configuration
VEHICLE_SPEED_TOPIC = "vehicle/speed"
DOOR_STATE_TOPIC = "door/changestate"


class FirebaseSpeedSubscriberApp(VehicleApp):
    """
    FirebaseSpeedSubscriberApp listens for speed data on the MQTT topic "vehicle/speed"
    and writes it to Firebase Firestore. It also listens for door state changes in
    Firestore and publishes those changes to an MQTT topic.
    """

    def __init__(self, vehicle_client: Vehicle):
        super().__init__()
        self.vehicle = vehicle_client
        self.create_firebase_service_account_json()
        cred = credentials.Certificate("app/src/firebase/admin_cred.json")
        firebase_admin.initialize_app(cred)
        self.db = firestore.client()
        self.last_door_state = False

    async def on_start(self):
        """Main app start function to initiate tasks."""
        logger.info("FirebaseSpeedSubscriberApp started.")

        # Run both tasks concurrently
        await asyncio.gather(
            self.poll_door_state(),
            self.vehicle.Speed.subscribe(self.on_speed_changed),
        )

    async def poll_door_state(self):
        """Polls Firestore at intervals to check the isEngineRunning field."""
        doc_ref = self.db.collection("SDV").document("Vehicle")

        while True:
            try:
                # Fetch the latest document snapshot
                doc_snapshot = doc_ref.get()
                data = doc_snapshot.to_dict()
                is_engine_running = data.get("isEngineRunning", False)  # type: ignore
                # Check if the door state has changed
                if is_engine_running != self.last_door_state:
                    logger.info(f"Door state changed to: {is_engine_running}")
                    self.last_door_state = is_engine_running
                    await self.publish_door_state(is_engine_running)

            except Exception as e:
                logger.error(f"Failed to fetch door state from Firestore: {e}")

            # Wait for a defined interval before polling again (e.g., every 5 seconds)
            await asyncio.sleep(1)

    async def publish_door_state(self, is_open):
        """Publishes door state to MQTT topic using Velocitas publish_event."""
        door_state = "open" if is_open else "closed"
        await self.publish_event(
            DOOR_STATE_TOPIC,
            json.dumps({"door_state": door_state}),
        )

        logger.info(
            f"Published door state '{door_state}' to MQTT topic '{DOOR_STATE_TOPIC}'."
        )
        await asyncio.sleep(0.1)  # Small delay to ensure non-blocking operation

    async def on_speed_changed(self, data: DataPointReply):
        """Handles vehicle speed updates and pushes them to Firestore."""
        speed = data.get(self.vehicle.Speed).value
        await self.update_speed_in_firebase(speed)

    async def update_speed_in_firebase(self, speed):
        """Updates the latest vehicle speed in Firebase Firestore."""
        try:
            doc_ref = self.db.collection("SDV").document("Vehicle")
            doc_ref.update({"speed": speed})
            logger.info(f"Vehicle speed updated in Firebase to: {speed} km/h")
        except Exception as e:
            logger.error(f"Failed to update Firebase with speed: {e}")

    def create_firebase_service_account_json(self):
        """Creates the Firebase service account JSON from environment variables."""
        file_path = "app/src/firebase/admin_cred.json"

        # Check if the file already exists
        if os.path.exists(file_path):
            print(f"{file_path} already exists. Skipping creation.")
            os.remove(file_path)

        b = "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAic2R2LWRiIiwKICAicHJpdmF0ZV9rZXlfaWQiOiAiNjIzZDgwN2E5NDQ4N2VkNDE0MTMzMzZlNTA1N2U1MzJjNDY5OGQ5NCIsCiAgImNsaWVudF9lbWFpbCI6ICJmaXJlYmFzZS1hZG1pbnNkay0xaGFtMEBzZHYtZGIuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJjbGllbnRfaWQiOiAiMTA1MjkzOTk0MTE5MTcyODI1MjM1IiwKICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICJ0b2tlbl91cmkiOiAiaHR0cHM6Ly9vYXV0aDIuZ29vZ2xlYXBpcy5jb20vdG9rZW4iLAogICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwKICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9maXJlYmFzZS1hZG1pbnNkay0xaGFtMCU0MHNkdi1kYi5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgInVuaXZlcnNlX2RvbWFpbiI6ICJnb29nbGVhcGlzLmNvbSIKfQo="
        data = self.decode_base64(b)
        data = json.loads(data)

        # Retrieve environment variables and replace \n to make it an actual newline
        key1 = os.getenv("FBKEY", "").replace("\\n", "\n")
        key2 = os.getenv("FBKEY2", "").replace("\\n", "\n")
        fb_id = os.getenv("FB_PRIVATE_ID", "")

        # Concatenate the keys into a single private key
        private_key = key1 + key2

        # Insert the values into the JSON structure
        data["private_key_id"] = fb_id
        data["private_key"] = private_key

        print(data)
        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)
        print(f"{file_path} has been created.")

    def decode_base64(self, encoded_string):
        """Decode a base64 encoded string to UTF-8."""
        decoded_bytes = base64.b64decode(encoded_string)
        return decoded_bytes.decode("utf-8")


async def main():
    """Main entry point for the Vehicle App."""
    logger.info("Starting FirebaseSpeedSubscriberApp...")
    vehicle_app = FirebaseSpeedSubscriberApp(vehicle)
    await vehicle_app.run()


if __name__ == "__main__":
    asyncio.run(main())
