import datetime
import json
import typing
from logging import DEBUG
from typing import Optional

from google.cloud.trace_v2 import TraceServiceClient  # type: ignore
from google.cloud.trace_v2.services.trace_service.transports import TraceServiceGrpcTransport  # type: ignore
from google.oauth2 import service_account  # type: ignore
from google.cloud import bigquery  # type: ignore
from opentelemetry import trace
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter, _OPTIONS
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.trace import TracerProvider, ReadableSpan
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace import Tracer

from src.utils import config, logging


class LogOut:
    def __init__(self, level: int):
        self.level = level
        self.logger = logging.initialize_logger("opentelemetry")

    def write(self, value: str):
        self.logger.log(self.level, value)

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class FileOut:
    def __init__(self, path: str):
        self.path = path

        try:
            with open(path, "x") as file:
                file.write("[\n")
        except Exception:
            pass

    def write(self, value: str):
        self.file.write(value + ",\n")

    def __enter__(self):
        self.file = open(self.path, "a")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()


class BigQueryOut:
    def __init__(self, experiment: str):
        self.experiment = experiment

        credentials = service_account.Credentials.from_service_account_info(
            json.loads(str(config.get("secret.gcp.bigquery.credentials"))),
        )
        self.bq = bigquery.Client(credentials=credentials)

    def write(self, value: str):
        json_value = json.loads(value)
        self.rows.append({
            "experiment": self.experiment,
            "name": json_value["name"],
            "attributes": json.dumps(json_value["attributes"]),
            "start_time": json_value["start_time"][:-1],
            "end_time": json_value["end_time"][:-1],
            "context": {
                "trace_id": json_value["context"]["trace_id"],
                "span_id": json_value["context"]["span_id"],
                "trace_state": json_value["context"]["trace_state"],
            },
            "parent_id": json_value["parent_id"],
        })

    def __enter__(self):
        self.rows = []

    def __exit__(self, exc_type, exc_val, exc_tb):
        errors = self.bq.insert_rows_json(config.get("gcp.bigquery.opentelemetry"), self.rows)
        if errors:
            logging.lwarn(f"BigQuery encountered errors while inserting rows: {errors}")


class LocalSpanExporter(SpanExporter):
    """Implementation of :class:`SpanExporter` that prints spans to a local source."""

    def _format(self, span: ReadableSpan) -> str:
        full = json.loads(span.to_json())
        partial = {}

        for key in ["name", "attributes", "start_time", "end_time", "context", "parent_id"]:
            partial[key] = full[key]

        return json.dumps(partial)

    def __init__(
        self,
        out: typing.Union[LogOut, FileOut, BigQueryOut],
        service_name: Optional[str] = None,
        formatter: typing.Optional[
            typing.Callable[
                [ReadableSpan], str,
            ]
        ] = None,
    ):
        self.out = out
        self.service_name = service_name
        if formatter is not None:
            self.formatter = formatter
        else:
            self.formatter = self._format

    def export(self, spans: typing.Sequence[ReadableSpan]) -> SpanExportResult:
        with self.out:
            for span in spans:
                self.out.write(self.formatter(span))
        return SpanExportResult.SUCCESS

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


def initialize() -> Tracer:
    tracer_provider = TracerProvider()
    experiment_name = f"{config.get('experiment.telemetry.name', 'opentelemetry')}_{datetime.date.today().isoformat()}"

    if config.get("secret.gcp.trace.credentials", ignore_errors=True) is not None:
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(str(config.get("secret.gcp.trace.credentials"))),
        )
        cloud_trace_exporter = CloudTraceSpanExporter(
            config.get("gcp.project_id"),
            TraceServiceClient(
                transport=TraceServiceGrpcTransport(
                    channel=TraceServiceGrpcTransport.create_channel(
                        options=_OPTIONS,
                        credentials=credentials,
                    ),
                ),
            ),
        )

        tracer_provider.add_span_processor(BatchSpanProcessor(cloud_trace_exporter))

    if config.get("secret.gcp.bigquery.credentials", ignore_errors=True) is not None:
        tracer_provider.add_span_processor(BatchSpanProcessor(
            LocalSpanExporter(BigQueryOut(experiment_name)),
        ))

    # tracer_provider.add_span_processor(BatchSpanProcessor(
    #     LocalSpanExporter(LogOut(DEBUG)),
    # ))
    # tracer_provider.add_span_processor(BatchSpanProcessor(
    #     LocalSpanExporter(FileOut(
    #         f"logs/{experiment_name}.json",
    #     )),
    # ))

    RequestsInstrumentor().instrument(tracer_provider=tracer_provider)

    trace.set_tracer_provider(tracer_provider)

    return trace.get_tracer(config.get("path"))
