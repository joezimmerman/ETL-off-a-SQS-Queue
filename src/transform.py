import hashlib
from typing import List, Dict, Any, Optional
from datetime import datetime


def mask_ip(ip: str) -> str:
    return hashlib.md5(ip.encode()).hexdigest()


def mask_device_id(device_id: str) -> str:
    return hashlib.md5(device_id.encode()).hexdigest()


def mask_data(data: Dict[str, Any]) -> Dict[str, Any]:
    masked_data = {
        "user_id": data["user_id"],
        "app_version": data["app_version"],
        "device_type": data["device_type"],
        "masked_ip": mask_ip(data["ip"]),
        "locale": data["locale"],
        "masked_device_id": mask_device_id(data["device_id"]),
        # Added this line to include the create_date field
        "create_date": datetime.utcnow().date()
    }
    return masked_data

# Function to mask PII data in the input dictionary
def mask_pii_data(data: Dict[str, Any]) -> Dict[str, Any]:
    # Extract values from the input data
    user_id = data.get("user_id")
    device_type = data.get("device_type")
    ip = data.get("ip")
    device_id = data.get("device_id", "unknown")
    locale = data.get("locale", "unknown")
    app_version = data.get("app_version")

    print(f"Raw data: {data}")

    # Check for required fields in the input data
    if user_id is None or device_type is None or ip is None or app_version is None:
        print(f"Invalid data: {data}")
        return None

    # Mask the IP and device_id using SHA-256 hashing
    masked_ip = hashlib.sha256(ip.encode("utf-8")).hexdigest()
    masked_device_id = hashlib.sha256(device_id.encode("utf-8")).hexdigest()

    # Create a dictionary with the masked data
    masked_data = {
        "user_id": user_id,
        "device_type": device_type,
        "masked_ip": masked_ip,
        "masked_device_id": masked_device_id,
        "locale": locale,
        "app_version": app_version,
    }

    print(f"Masked data: {masked_data}")

    # Return the masked data dictionary along with the current timestamp as create_date
    return {
        "user_id": user_id,
        "device_type": device_type,
        "masked_ip": masked_ip,
        "masked_device_id": masked_device_id,
        "locale": locale,
        "app_version": app_version,
        "create_date": datetime.now(),
    }