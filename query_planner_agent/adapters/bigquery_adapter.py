import os
import logging
from typing import Any, Dict, List, Optional, Tuple, cast

from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter, ArrayQueryParameter
from google.oauth2 import service_account

from common_interfaces import (
    CommonDataAccessInterface,
    QueryObject,
    QueryResult,
    SchemaInfo,
    ConnectionError,
    QueryExecutionError,
    SchemaError,
    ConfigurationError,
    TableSchema,
    SchemaColumn,
)

logger = logging.getLogger("bigquery_adapter")


class BigQueryAdapter(CommonDataAccessInterface):
    """Google BigQuery implementation of *CommonDataAccessInterface* with full
    Pylance‑/mypy‑clean type hints.
    """

    def __init__(self) -> None:  # noqa: D401  (simple description in docstring)
        self.client: Optional[bigquery.Client] = None
        self.project_id: Optional[str] = None
        self.default_dataset_id: Optional[str] = None
        self.default_table_id: Optional[str] = None

    # ───────────────────────────────────────────────────────── setup / teardown ─┐
    def connect(self, config: Dict[str, Any]) -> None:                             # │
        """Establish a BigQuery client using either ADC or an explicit SA JSON."""
        self.project_id = cast(
            str, config.get("project_id") or os.getenv("GCP_PROJECT_ID")
        )
        if not self.project_id:
            raise ConfigurationError(
                "BigQueryAdapter config missing 'project_id' and GCP_PROJECT_ID env var not set."
            )

        self.default_dataset_id = cast(Optional[str], config.get("dataset_id"))
        self.default_table_id = cast(Optional[str], config.get("table_id"))

        credentials_path: Optional[str] = cast(Optional[str], config.get("credentials_path"))
        try:
            if credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path
                )
                self.client = bigquery.Client(
                    project=self.project_id, credentials=credentials
                )
                logger.info(
                    "BigQuery client initialised for %s (service‑account creds).",
                    self.project_id,
                )
            else:
                self.client = bigquery.Client(project=self.project_id)
                logger.info("BigQuery client initialised for %s using ADC.", self.project_id)

            # tiny smoke‑test
            _ = list(self.client.list_datasets(max_results=1))
            logger.info("Successfully connected to BigQuery project: %s", self.project_id)
        except Exception as exc:
            self.client = None
            raise ConnectionError(
                f"Failed to connect to BigQuery project {self.project_id}: {exc}"
            ) from exc

    def disconnect(self) -> None:                                                   # │
        if self.client is None:
            logger.info("BigQueryAdapter.disconnect called but no active client.")
            return
        # python‑bigquery manages underlying resources; nothing to close.
        logger.info("Released BigQuery client for project %s", self.project_id)
        self.client = None

    # ─────────────────────────────────────────────────────────────── queries ─────┐
    def execute_query(self, query_object: QueryObject) -> QueryResult:              # │
        if self.client is None:
            raise ConnectionError("Not connected to BigQuery. Call connect() first.")
        client: bigquery.Client = cast(bigquery.Client, self.client)

        logger.info(
            "Executing BigQuery query: %s | params=%s",
            query_object.query_string,
            query_object.parameters,
        )

        # build QueryJobConfig with typed parameters
        job_config: Optional[bigquery.QueryJobConfig] = None
        if query_object.parameters:
            query_params: List[ScalarQueryParameter | ArrayQueryParameter] = []
            for key, value in query_object.parameters.items():
                # Arrays need ArrayQueryParameter, scalars -> ScalarQueryParameter
                if isinstance(value, list):
                    # naive element‑type inference
                    elem_type: str = "STRING"
                    if value:
                        first = value[0]
                        if isinstance(first, int):
                            elem_type = "INT64"
                        elif isinstance(first, float):
                            elem_type = "FLOAT64"
                        elif isinstance(first, bool):
                            elem_type = "BOOL"
                    query_params.append(
                        bigquery.ArrayQueryParameter(key, elem_type, value)
                    )
                else:
                    query_params.append(
                        bigquery.ScalarQueryParameter(key, None, value)  # type: ignore[arg-type]
                    )
            job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        try:
            query_job = client.query(query_object.query_string, job_config=job_config)
            results = query_job.result()  # blocks

            columns: List[str] = [f.name for f in results.schema] if results.schema else []
            rows_as_dicts: List[Dict[str, Any]] = [dict(r.items()) for r in results]

            affected: Optional[int]
            if query_object.query_type.lower() == "select":
                affected = results.total_rows or len(rows_as_dicts)
            else:
                affected = getattr(query_job, "num_dml_affected_rows", None)

            return QueryResult(
                success=True,
                columns=columns,
                rows=rows_as_dicts,
                row_count=affected,
                metadata={
                    "job_id": query_job.job_id,
                    "location": query_job.location,
                    "bytes_billed": query_job.total_bytes_billed,
                    "cache_hit": query_job.cache_hit,
                },
                error_message=None,
            )
        except Exception as exc:
            logger.exception("BigQuery query execution failed: %s", exc)
            raise QueryExecutionError(f"BigQuery execution error: {exc}") from exc

    # ─────────────────────────────────────────────────────────────── schema ──────┐
    def get_schema_information(self, entity_name: Optional[str] = None) -> SchemaInfo:
        if self.client is None:
            raise ConnectionError("Not connected to BigQuery. Call connect() first.")
        client: bigquery.Client = cast(bigquery.Client, self.client)

        tables_data: List[TableSchema] = []
        try:
            if entity_name:
                ds_id, tbl_id = self._resolve_entity_name(entity_name)
                tables_data.append(self._fetch_single_table_schema(client, ds_id, tbl_id))
            elif self.default_dataset_id:
                for tbl in client.list_tables(self.default_dataset_id):
                    tables_data.append(
                        self._fetch_single_table_schema(client, tbl.dataset_id, tbl.table_id)
                    )
            else:
                return SchemaInfo(
                    tables=None,
                    raw_schema=None,
                    error_message="Cannot list tables without default_dataset_id or entity_name.",
                )

            return SchemaInfo(
                tables=tables_data or None,
                raw_schema=None,
                error_message=None,
            )
        except Exception as exc:
            logger.exception("Unexpected error during BigQuery schema retrieval: %s", exc)
            raise SchemaError(f"Unexpected BigQuery schema error: {exc}") from exc

    # ─────────────────────────────────────────────────────────── helpers ─────────┐
    def _resolve_entity_name(self, entity_name: str) -> Tuple[str, str]:
        parts = entity_name.split(".")
        if len(parts) == 2:  # dataset.table
            return parts[0], parts[1]
        if len(parts) == 1 and self.default_dataset_id:
            return self.default_dataset_id, parts[0]
        if len(parts) >= 3:  # maybe project.dataset.table
            return parts[-2], parts[-1]
        raise SchemaError(
            f"Cannot determine dataset for table '{entity_name}'. Provide dataset.table or set default_dataset_id."
        )

    def _fetch_single_table_schema(
        self, client: bigquery.Client, dataset_id: str, table_id: str
    ) -> TableSchema:
        try:
            table_ref = client.dataset(dataset_id).table(table_id)
            table_obj = client.get_table(table_ref)
            columns = [
                SchemaColumn(
                    name=f.name,
                    type=str(f.field_type or ""),
                    required=(f.mode == "REQUIRED"),
                )
                for f in table_obj.schema
            ]
            return TableSchema(
                table_name=f"{dataset_id}.{table_id}",
                columns=columns,
                description=table_obj.description,
            )
        except Exception as exc:
            logger.error(
                "Failed to get schema for BigQuery table %s.%s: %s", dataset_id, table_id, exc
            )
            raise SchemaError(
                f"Failed to retrieve schema for BigQuery table '{dataset_id}.{table_id}': {exc}"
            ) from exc