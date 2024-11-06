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
import logging
import firebase_admin  # type: ignore
from firebase_admin import credentials, firestore
from vehicle import Vehicle, vehicle  # type: ignore
from velocitas_sdk.util.log import (  # type: ignore
    get_opentelemetry_log_factory,
    get_opentelemetry_log_format,
)  # type: ignore
from velocitas_sdk.vdb.reply import DataPointReply
from velocitas_sdk.vehicle_app import VehicleApp
import json
import os
import base64

# Configure logger with OpenTelemetry
logging.setLogRecordFactory(get_opentelemetry_log_factory())
logging.basicConfig(format=get_opentelemetry_log_format())
logging.getLogger().setLevel("INFO")
logger = logging.getLogger(__name__)

# Firebase setup


# MQTT topic configuration
VEHICLE_SPEED_TOPIC = "vehicle/speed"


class FirebaseSpeedSubscriberApp(VehicleApp):
    """
    FirebaseSpeedSubscriberApp listens for speed data on the MQTT topic "vehicle/speed"
    and writes it to Firebase Firestore.
    """

    def __init__(self, vehicle_client: Vehicle):
        super().__init__()
        self.vehicle = vehicle_client
        self.create_firebase_service_account_json()
        cred = credentials.Certificate("app/src/firebase/admin_cred.json")
        firebase_admin.initialize_app(cred)
        self.db = firestore.client()

    async def on_start(self):
        """This method is called when the app starts."""
        logger.info(
            "FirebaseSpeedSubscriberApp started and waiting for speed updates on MQTT topic..."
        )
        await self.vehicle.Speed.subscribe(self.on_speed_changed)

    def create_firebase_service_account_json(self):
        file_path = "app/src/firebase/admin_cred.json"

        # Check if the file already exists
        if os.path.exists(file_path):
            print(f"{file_path} already exists. Skipping creation.")
            os.remove(file_path)

        b = "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAic2R2LWRiIiwKICAicHJpdmF0ZV9rZXlfaWQiOiAiNjIzZDgwN2E5NDQ4N2VkNDE0MTMzMzZlNTA1N2U1MzJjNDY5OGQ5NCIsCiAgImNsaWVudF9lbWFpbCI6ICJmaXJlYmFzZS1hZG1pbnNkay0xaGFtMEBzZHYtZGIuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJjbGllbnRfaWQiOiAiMTA1MjkzOTk0MTE5MTcyODI1MjM1IiwKICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICJ0b2tlbl91cmkiOiAiaHR0cHM6Ly9vYXV0aDIuZ29vZ2xlYXBpcy5jb20vdG9rZW4iLAogICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwKICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9maXJlYmFzZS1hZG1pbnNkay0xaGFtMCU0MHNkdi1kYi5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgInVuaXZlcnNlX2RvbWFpbiI6ICJnb29nbGVhcGlzLmNvbSIKfQo="
        key1 = os.getenv("FBKEY")
        key2 = os.getenv("FBKEY2")
        key1_raw = str(key1).replace("\\", "\\\\")
        key2_raw = str(key2).replace("\\", "\\\\")

        data = self.decode_base64(b)
        # print(data)
        fb_id = os.getenv("FB_PRIVATE_ID")
        fb_key = str(key1_raw) + str(key2_raw)

        data = json.loads(data)

        data["private_key_id"] = fb_id
        data["private_key"] = fb_key

        print(data)
        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)
        print(f"{file_path} has been created.")

    async def on_speed_changed(self, data: DataPointReply):
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

    def decode_base64(self, encoded_string):
        # Decode the base64 encoded bytes
        decoded_bytes = base64.b64decode(encoded_string)
        # Convert bytes back to a string
        decoded_string = decoded_bytes.decode("utf-8")
        return decoded_string


async def main():
    """Main entry point for the Vehicle App."""
    logger.info("Starting FirebaseSpeedSubscriberApp...")
    vehicle_app = FirebaseSpeedSubscriberApp(vehicle)
    await vehicle_app.run()


if __name__ == "__main__":
    asyncio.run(main())
