import json
import sys
import base64
from flask import Flask, request
from google.cloud import storage

SOURCE_BUCKETS = ["george.black", "media.george.black"]

MIME_TYPES_MAP = {
    "aac": "audio/aac",
    "arc": "application/x-freearc",
    "avi": "video/x-msvideo",
    "avif": "image/avif",
    "css": "text/css",
    "csv": "text/csv",
    "doc": "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "gz": "application/gzip",
    "gpx": "application/gpx+xml",
    "gif": "image/gif",
    "html": "text/html",
    "ico": "image/vnd.microsoft.icon",
    "ics": "text/calendar",
    "jpeg": "image/jpeg",
    "jpg": "image/jpeg",
    "js": "text/javascript",
    "json": "application/json; charset=utf-8",
    "mid": "audio/x-midi",
    "midi": "audio/x-midi",
    "mpeg": "video/mpeg",
    "png": "image/png",
    "pdf": "application/pdf",
    "rar": "application/vnd.rar",
    "rtf": "application/rtf",
    "sh": "application/x-sh",
    "svg": "image/svg+xml",
    "tar": "application/x-tar",
    "tif": "image/tiff",
    "tiff": "image/tiff",
    "txt": "text/plain",
    "usdz": "model/usd",
    "wav": "audio/wav",
    "weba": "audio/webm",
    "webm": "video/webm",
    "webp": "image/webp",
    "xhtml": "application/xhtml+xml",
    "xml": "application/xml",
    "zip": "application/zip",
}

app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    envelope = request.get_json()
    if not envelope:
        print("Error: No Pub/Sub message received")
        return "Bad Request: No Pub/Sub message received", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        print("Error: Invalid Pub/Sub message format")
        return "Bad Request: Invalid Pub/Sub message format", 400

    message = envelope["message"]

    if (
        not isinstance(message, dict)
        or not message["data"]
        or not message["attributes"]
    ):
        print("Error: Invalid Pub/Sub message format")
        return "Bad Request: Invalid Pub/Sub message format", 400

    event_type = message["attributes"]["eventType"]
    if event_type != "OBJECT_FINALIZE":
        print(f"Ignoring unrelated Cloud Storage event type: {event_type}")
        return "No action taken", 200

    try:
        data = json.loads(base64.b64decode(message["data"]).decode())
    except Exception as e:
        print("Error: Data property is not valid base64 encoded JSON")
        return "Bad Request: Data property is not valid base64 encoded JSON", 400

    if not data["name"] or not data["bucket"]:
        print(f"Error: Expected name/bucket in notification")
        return f"Bad Request: Expected name/bucket in notification", 400

    if data["bucket"] not in SOURCE_BUCKETS:
        print(f"Ignoring event from bucket {data['bucket']}")
        return ("", 200)

    try:
        update_object_metadata(data)
        sys.stdout.flush()

    except Exception as e:
        print(f"Error: {e}")
        return ("", 500)

    return ("Metadata updated", 200)


def update_object_metadata(data):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(data["bucket"])
    blob = bucket.get_blob(data["name"])

    content_type = get_content_type(data["name"])
    cache_control = get_cache_control(data["name"])

    if not blob.exists():
        print(f"Object does not exist: {data['name']}")
        return

    blob.content_type = content_type
    blob.cache_control = cache_control
    blob.patch()
    print(f"Successfully updated object: {data['name']}")


def get_cache_control(object_name):
    seconds = "2592000"
    extension = object_name.split(".").pop()

    if extension in ["html", "xml", "json", "txt"]:
        seconds = "900"
    elif extension in ["js", "css"]:
        seconds = "172800"

    return f"public, max-age={seconds}"


def get_content_type(object_name):
    extension = object_name.split(".").pop()
    if extension in MIME_TYPES_MAP.keys():
        return MIME_TYPES_MAP[extension]
    return "application/octet-stream"
