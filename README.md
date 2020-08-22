# GCS Meta Modifier

This service will modify Google Cloud Storage object metadata as soon as the object has been uploaded.

Setup:

1. Attach Cloud Pub/Sub topic to bucket
2. Subscribe service to Pub/Sub topic
3. Service will update object's metadata on `OBJECT_FINALIZE` event

The original purpose of this code was to modify the `Cache-Control` metadata of objects served by Cloud CDN.
