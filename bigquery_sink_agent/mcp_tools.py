import os,json
from google.cloud import bigquery

class BigQuerySinkTools:
    def __init__(self):
        # Get BigQuery target from environment
        self.project_id = os.getenv("GCP_PROJECT_ID")  # optional, if using ADC, project can be inferred
        self.dataset_id = os.getenv("BQ_DATASET")
        self.table_id = os.getenv("BQ_TABLE")
        if not self.dataset_id or not self.table_id:
            raise RuntimeError("BQ_DATASET and BQ_TABLE must be set for bigquery_sink_agent")
        # Construct full table identifier for BigQuery API      
        self.table_full = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
        # Initialize BigQuery client (Application Default Credentials are used)
        self.client = bigquery.Client()

    def insert_log(self, log_entry: str) -> dict:
        """
        Insert a single JSON‐string log_entry into the JSON column.
        """
        # Wrap in json.dumps to produce a JSON string
        row = {"log_entry": json.dumps(json.loads(log_entry))}
        errors = self.client.insert_rows_json(self.table_full, [row])
        if errors:
            return {"error": errors}
        return {"inserted": 1}
    
    def insert_logs(self, log_entries: list[str]) -> dict:
        """
        Insert multiple JSON‐string log_entries.
        """
        rows = []
        for e in log_entries:
            # parse then re-dump to ensure proper JSON string
            obj = json.loads(e)
            rows.append({"log_entry": json.dumps(obj)})
        errors = self.client.insert_rows_json(self.table_full, rows)
        if errors:
            return {"error": errors}
        return {"inserted": len(rows)}