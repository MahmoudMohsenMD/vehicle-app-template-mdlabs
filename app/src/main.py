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

        data = {
            "type": "service_account",
            "project_id": "sdv-db",
            "private_key_id": "623d807a94487ed41413336e5057e532c4698d94",
            "private_key": """-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCcZU8Loz4M2WbX\nrSfYtM2eZ4CcnjrKwTeuAYOMgnQRuhJ+lchQLpko5Xdk2Ij49IM9PZgCbKgga4Ms\ndwuhUjo5pPUZGEJy4g8kTusqM+3m5/d/dmMvUPErFn24X/r9/YVXkhOXrhStC/CR\ne8g1tE9MZ5A0NNrnVd5GL2ARnhnPwmrJ/CaxFh7dJnMzlSUabxSBCIXTaLuqlb5M\nz/J+9xAgHOWIfYpfIFymV4WKbqfSF6Kot70cz9VRWrA2CBAcI1n8oXuTpDEVfFbh\nctqRgNTIQmScEqV/ySfZsEm2eDGX/iM2YmhbTcMOBEJNIEFdPx366U/itY6m5a6s\n2fZZ4NyNAgMBAAECggEAGUf/KSBwTV9vUnn5MPy5KFwO3EJvuorVV1mDTFliLeNo\nTfIkeWGu/H9rd2/RZH0SIfos8mdiHiuC/tMXnDJQmzThMfdL9oo4bGQde6r9UBA7\nX+1hRyrMcWynUgCh8CDlRyyj4icPzJFbiAj8akIFd5JeKzJIGJE25x2NaAuonH7O\nVxS5f0iPFxoJ/S9tsp0YtYvY/9xDR5EBdTvS4lALBs6/YtPM5287I+kCUuaJYSjc\nMznue6gsRrCwsBulF8ueI+TQNEEoZOea0hbyzAohyJt8p59VGLEFdIDF/UQJmmRp\nr6Z2Tp+oD0f6FrM05LiB5SGYLCUWamCfsHq2RVetOQKBgQDJpz2BnGFMCgtXlOBY\n5njJ5KLw1wd8aKpIQp+5oEXTktpJEanY/ATyOZ1CPQeyet1J+svs9uShAa+Mzjgk\nInX02uBXQl+ORakG4iFICVZ4ovZI1UjoH7sKRRAOZVg7lcgWIftENtmBVPEr6QcU\nD2ug5+pMTgQzDXivI5mpnPEXbwKBgQDGi5frTZaoNNlB0ZxrG8c8MXqj7TjpuAeQ\nX5p+zIvk3KusnczJz9g5ZCtxv7p26zTN087e8mJKCduUSIj+ZvscKp++dkFh2xYP\n0tAgEHrVGI5WX4I+USq5ETaSdB1Uh76UTatiEwIWJ9gkGaPX6HIjewNVBZuYmo+A\n72jUcfqtwwKBgQDEvUog3jV/Pn16YWHQ2msWbt8aInDHTEZ2Yan8LQZzOV+G0pjn\nflpFCTkImwqbc4va1bmH5AmDnGVt9vSHLKfw0Gr4N2WLJSOX95+I7LNU14l53b1X\n2z1c86yKnw3KdIPWNCk1kF389KantJ6XT8QuJJZPA/7M4GPSW+VbuibuOQKBgDLe\nWwr/cwUfnTY+1RugH1ituKU9RyKictWrmPK5x3HYk+eC+pAOP3Dc2ECphcWoF3xQ\nsyTt5v7fLbH9L5Q2oEmkJg9vTusabbmaIpbEgiQMyZMZn04GD7Mg1OGK/GTG7q4i\nhDF5e0Rf6wG8y/yqYmvyvXTQHgBoQTIgpLQhi79vAoGAKb2xoeMJpy1zTDd91yls\n801l6GsYD7O3ocedoMUGl3FVNhhE5vQXMROgLvth2p7mYTHzVazBUeG6GBRuDavl\nff33wMbcqbJ7WWJ2GYMTzGgq3zYFm+NXJFzSQpW1Z3G5UQ+c8pa1XGdy4CyB3erS\ngkDkv6KkWuZQpjHHt/jhFJM=\n-----END PRIVATE KEY-----\n""",
            "client_email": "firebase-adminsdk-1ham0@sdv-db.iam.gserviceaccount.com",
            "client_id": "105293994119172825235",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-1ham0%40sdv-db.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com",
        }

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


async def main():
    """Main entry point for the Vehicle App."""
    logger.info("Starting FirebaseSpeedSubscriberApp...")
    vehicle_app = FirebaseSpeedSubscriberApp(vehicle)
    await vehicle_app.run()


if __name__ == "__main__":
    asyncio.run(main())