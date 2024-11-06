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
            return

        b = "ewogICAgICAgICAgICAidHlwZSI6ICJzZXJ2aWNlX2FjY291bnQiLAogICAgICAgICAgICAicHJvamVjdF9pZCI6ICJzZHYtZGIiLAogICAgICAgICAgICAicHJpdmF0ZV9rZXlfaWQiOiAiNjIzZDgwN2E5NDQ4N2VkNDE0MTMzMzZlNTA1N2U1MzJjNDY5OGQ5NCIsCiAgICAgICAgICAgICJwcml2YXRlX2tleSI6ICIiIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuTUlJRXZRSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS2N3Z2dTakFnRUFBb0lCQVFDY1pVOExvejRNMldiWFxuclNmWXRNMmVaNENjbmpyS3dUZXVBWU9NZ25RUnVoSitsY2hRTHBrbzVYZGsySWo0OUlNOVBaZ0NiS2dnYTRNc1xuZHd1aFVqbzVwUFVaR0VKeTRnOGtUdXNxTSszbTUvZC9kbU12VVBFckZuMjRYL3I5L1lWWGtoT1hyaFN0Qy9DUlxuZThnMXRFOU1aNUEwTk5yblZkNUdMMkFSbmhuUHdtckovQ2F4Rmg3ZEpuTXpsU1VhYnhTQkNJWFRhTHVxbGI1TVxuei9KKzl4QWdIT1dJZllwZklGeW1WNFdLYnFmU0Y2S290NzBjejlWUldyQTJDQkFjSTFuOG9YdVRwREVWZkZiaFxuY3RxUmdOVElRbVNjRXFWL3lTZlpzRW0yZURHWC9pTTJZbWhiVGNNT0JFSk5JRUZkUHgzNjZVL2l0WTZtNWE2c1xuMmZaWjROeU5BZ01CQUFFQ2dnRUFHVWYvS1NCd1RWOXZVbm41TVB5NUtGd08zRUp2dW9yVlYxbURURmxpTGVOb1xuVGZJa2VXR3UvSDlyZDIvUlpIMFNJZm9zOG1kaUhpdUMvdE1YbkRKUW16VGhNZmRMOW9vNGJHUWRlNnI5VUJBN1xuWCsxaFJ5ck1jV3luVWdDaDhDRGxSeXlqNGljUHpKRmJpQWo4YWtJRmQ1SmVLekpJR0pFMjV4Mk5hQXVvbkg3T1xuVnhTNWYwaVBGeG9KL1M5dHNwMFl0WXZZLzl4RFI1RUJkVHZTNGxBTEJzNi9ZdFBNNTI4N0kra0NVdWFKWVNqY1xuTXpudWU2Z3NSckN3c0J1bEY4dWVJK1RRTkVFb1pPZWEwaGJ5ekFvaHlKdDhwNTlWR0xFRmRJREYvVVFKbW1ScFxucjZaMlRwK29EMGY2RnJNMDVMaUI1U0dZTENVV2FtQ2ZzSHEyUlZldE9RS0JnUURKcHoyQm5HRk1DZ3RYbE9CWVxuNW5qSjVLTHcxd2Q4YUtwSVFwKzVvRVhUa3RwSkVhblkvQVR5T1oxQ1BRZXlldDFKK3N2czl1U2hBYStNempna1xuSW5YMDJ1QlhRbCtPUmFrRzRpRklDVlo0b3ZaSTFVam9IN3NLUlJBT1pWZzdsY2dXSWZ0RU50bUJWUEVyNlFjVVxuRDJ1ZzUrcE1UZ1F6RFhpdkk1bXBuUEVYYndLQmdRREdpNWZyVFphb05ObEIwWnhyRzhjOE1YcWo3VGpwdUFlUVxuWDVwK3pJdmszS3VzbmN6Sno5ZzVaQ3R4djdwMjZ6VE4wODdlOG1KS0NkdVVTSWorWnZzY0twKytka0ZoMnhZUFxuMHRBZ0VIclZHSTVXWDRJK1VTcTVFVGFTZEIxVWg3NlVUYXRpRXdJV0o5Z2tHYVBYNkhJamV3TlZCWnVZbW8rQVxuNzJqVWNmcXR3d0tCZ1FERXZVb2czalYvUG4xNllXSFEybXNXYnQ4YUluREhURVoyWWFuOExRWnpPVitHMHBqblxuZmxwRkNUa0ltd3FiYzR2YTFibUg1QW1EbkdWdDl2U0hMS2Z3MEdyNE4yV0xKU09YOTUrSTdMTlUxNGw1M2IxWFxuMnoxYzg2eUtudzNLZElQV05DazFrRjM4OUthbnRKNlhUOFF1SkpaUEEvN000R1BTVytWYnVpYnVPUUtCZ0RMZVxuV3dyL2N3VWZuVFkrMVJ1Z0gxaXR1S1U5UnlLaWN0V3JtUEs1eDNIWWsrZUMrcEFPUDNEYzJFQ3BoY1dvRjN4UVxuc3lUdDV2N2ZMYkg5TDVRMm9FbWtKZzl2VHVzYWJibWFJcGJFZ2lRTXlaTVpuMDRHRDdNZzFPR0svR1RHN3E0aVxuaERGNWUwUmY2d0c4eS95cVltdnl2WFRRSGdCb1FUSWdwTFFoaTc5dkFvR0FLYjJ4b2VNSnB5MXpURGQ5MXlsc1xuODAxbDZHc1lEN08zb2NlZG9NVUdsM0ZWTmhoRTV2UVhNUk9nTHZ0aDJwN21ZVEh6VmF6QlVlRzZHQlJ1RGF2bFxuZmYzM3dNYmNxYko3V1dKMkdZTVR6R2dxM3pZRm0rTlhKRnpTUXBXMVozRzVVUStjOHBhMVhHZHk0Q3lCM2VyU1xuZ2tEa3Y2S2tXdVpRcGpISHQvamhGSk09XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iIiIsCiAgICAgICAgICAgICJjbGllbnRfZW1haWwiOiAiZmlyZWJhc2UtYWRtaW5zZGstMWhhbTBAc2R2LWRiLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAgICAgICAgICAgImNsaWVudF9pZCI6ICIxMDUyOTM5OTQxMTkxNzI4MjUyMzUiLAogICAgICAgICAgICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICAgICAgICAgICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAgICAgICAgICAgImF1dGhfcHJvdmlkZXJfeDUwOV9jZXJ0X3VybCI6ICJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9vYXV0aDIvdjEvY2VydHMiLAogICAgICAgICAgICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9maXJlYmFzZS1hZG1pbnNkay0xaGFtMCU0MHNkdi1kYi5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgICAgICAgICAgICJ1bml2ZXJzZV9kb21haW4iOiAiZ29vZ2xlYXBpcy5jb20iLAogICAgICAgIH0="

        data = self.decode_base64(b)
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
