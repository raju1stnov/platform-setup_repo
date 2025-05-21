import os, json, logging, random
from datetime import datetime, timezone, timedelta
from google.protobuf.json_format import MessageToDict
from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2Client
from google.cloud import pubsub_v1

from typing import cast

PROJECT_ID= os.getenv("GCP_PROJECT_ID")
TOPIC_NAME= os.getenv("PUBSUB_TOPIC")
MAX_LOGS=int(os.getenv("MAX_LOGS", "100"))
if not PROJECT_ID or not TOPIC_NAME:
    raise RuntimeError("Environment variabls GCP_PROJECT_ID and PUBSUB_TOPIC must be set")

BASE_FILTER = f"""
resource.labels.project_id="{PROJECT_ID}"
severity >= INFO 
""".strip()

# BASE_FILTER = f"""
# severity>=ERROR
# resource.type="cloud_run_revision"
# resource.labels.environment_name="my_desired_env"
# resource.labels.project_id="myprojid"
# log_name="projects/myprojid/logs/airflow-scheduler"
# """.strip()

# Map severity levels to their names
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

_log_client=LoggingServiceV2Client()
_publisher= pubsub_v1.PublisherClient()
_topic_path=_publisher.topic_path(PROJECT_ID, TOPIC_NAME)

logger = logging.getLogger("log_ingest_agent.mcp")
logger.setLevel(logging.INFO)

def _safe_proto_to_dict(proto):
    try:
        return MessageToDict(proto)
    except TypeError as e:
        logger.warning("Failed to convert proto payload: %s", str(e))
        return {"error": str(e), "type_url": proto.TypeName()}

def fetch_logs() -> dict:
    """Fetch up to MAX_LOGS_TO_FETCH log entries that match LOG_FILTER."""
    # 1) build filter string adding a timestamp clause for the last 120 secs
    logging.info("Querying Cloud Logging with filter:\n%s", BASE_FILTER)
    cutoff=datetime.now(timezone.utc) - timedelta(seconds=604800)
    cutoff_str= cutoff.isoformat()
    time_clause= f'timestamp>= "{cutoff_str}"'
    final_filter= f"{BASE_FILTER} AND {time_clause}" if BASE_FILTER else time_clause
    logger.info("Listing logs with filter:\n%s", final_filter)

    # 2) query cloud logging 
    resp = _log_client.list_log_entries(
        request={
            "resource_names": [f"projects/{PROJECT_ID}"],
            "filter": final_filter,
            "page_size": MAX_LOGS,
        }
    )
    published=0
    logs = []
    for i, entry in enumerate(resp):
        if i >= MAX_LOGS:
            break

        # Convert logentry to a dictionary
        log_entry = {
            "timestamp": cast(datetime, entry.timestamp).isoformat() if entry.timestamp else None,
            "severity": SEVERITY_MAP.get(entry.severity, "UNKNOWN") if entry.severity else None,
            "log_name": entry.log_name,
            "resource": MessageToDict(entry.resource),
            "text_payload": entry.text_payload if entry.text_payload else None,
            "json_payload": MessageToDict(entry.json_payload) if entry.json_payload else None,            
            "proto_payload": _safe_proto_to_dict(entry.proto_payload) if entry.proto_payload else None,
        }

        try:
            _publisher.publish(
                _topic_path,
                data=json.dumps(log_entry).encode("utf-8"),
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
            published +=1
        except Exception as e:
            logger.error("Failed to publish entry #%d: %s", i, e, exc_info=True)
    logger.info("Total published: %d", published )
    return {"published": published}
      