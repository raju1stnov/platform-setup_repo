import os
import json
import logging
from datetime import datetime, timezone, timedelta

from google.protobuf.json_format import MessageToDict
from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2Client
from google.cloud import pubsub_v1

# ──────────────── Configuration ────────────────
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_NAME = os.getenv("PUBSUB_TOPIC")
MAX_LOGS = int(os.getenv("MAX_LOGS", "100"))

if not PROJECT_ID or not TOPIC_NAME:
    raise RuntimeError("Environment variables GCP_PROJECT_ID and PUBSUB_TOPIC must be set")

# Base filter (everything except the timestamp clause)
BASE_FILTER = """
severity>=ERROR
resource.type="cloud_run_revision"
resource.labels.environment_name="my_desired_env"
resource.labels.project_id="myprojid"
log_name="projects/myprojid/logs/airflow-scheduler"
""".strip()

# Severity mapping
SEVERITY_MAP = {
    0: "DEFAULT",
    100: "DEBUG",
    200: "INFO",
    300: "NOTICE",
    400: "WARNING",
    500: "ERROR",
    600: "CRITICAL",
    700: "ALERT",
    800: "EMERGENCY",
}

# Initialize clients once
_log_client = LoggingServiceV2Client()
_publisher = pubsub_v1.PublisherClient()
_topic_path = _publisher.topic_path(PROJECT_ID, TOPIC_NAME)

logger = logging.getLogger("log_ingest_agent.mcp")
logger.setLevel(logging.INFO)


def fetch_logs() -> dict:
    """
    Fetch log entries from the last 30 seconds using LoggingServiceV2Client,
    convert each entry exactly as in manual_log_pipeline_test.py, and publish 
    them one by one to the existing Pub/Sub topic.
    Returns {"published": <count>}.
    """
    # 1) Build the filter string, adding a timestamp clause for the last 30s
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=30)
    cutoff_str = cutoff.isoformat()
    time_clause = f'timestamp >= "{cutoff_str}"'
    final_filter = f"{BASE_FILTER} AND {time_clause}" if BASE_FILTER else time_clause

    logger.info("Listing logs with filter:\n%s", final_filter)

    # 2) Query Cloud Logging
    resp = _log_client.list_log_entries(
        request={
            "resource_names": [f"projects/{PROJECT_ID}"],
            "filter": final_filter,
            "page_size": MAX_LOGS,
        }
    )

    published = 0
    # 3) Convert & publish
    for i, entry in enumerate(resp):
        if i >= MAX_LOGS:
            break

        # Convert protobuf Timestamp to ISO string
        if entry.timestamp:
            ts = entry.timestamp.ToDatetime()
            timestamp_str = ts.isoformat()
        else:
            timestamp_str = None

        log_entry = {
            "timestamp": timestamp_str,
            "severity": SEVERITY_MAP.get(entry.severity, "UNKNOWN") if entry.severity is not None else None,
            "log_name": entry.log_name,
            "resource": MessageToDict(entry.resource),
            "text_payload": entry.text_payload if entry.text_payload else None,
            "json_payload": MessageToDict(entry.json_payload) if entry.json_payload else None,
            "proto_payload": MessageToDict(entry.proto_payload) if entry.proto_payload else None,
        }

        try:
            _publisher.publish(
                _topic_path,
                data=json.dumps(log_entry).encode("utf-8"),
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
            published += 1
        except Exception as e:
            logger.error("Failed to publish entry #%d: %s", i, e, exc_info=True)

    logger.info("Total published: %d", published)
    return {"published": published}
