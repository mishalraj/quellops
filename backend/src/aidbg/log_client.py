from __future__ import annotations

import grpc
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

from .logsrpc.logs.v1 import logs_pb2, logs_pb2_grpc


class LogClientError(Exception):
    def __init__(self, message: str, *, unavailable: bool = False):
        super().__init__(message)
        self.unavailable = unavailable


class LogClient:
    """Thin central-layer client. Provider knowledge stays in the Go service."""

    def __init__(self, target: str, token: str, timeout_s: float = 65):
        self.target = target
        self.token = token
        self.timeout_s = timeout_s
        self._channel = grpc.insecure_channel(target)
        self._stub = logs_pb2_grpc.LogsServiceStub(self._channel)

    @property
    def metadata(self):
        return (("authorization", "Bearer " + self.token),)

    @staticmethod
    def _dict(message) -> dict:
        result = MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
        # Protobuf JSON represents uint64 as strings. Keep the existing HTTP API
        # numeric for usage counters consumed by the UI.
        if "bytes_received" in result:
            result["bytes_received"] = int(result["bytes_received"])
        if "usage" in result and "bytes_received" in result["usage"]:
            result["usage"]["bytes_received"] = int(
                result["usage"]["bytes_received"]
            )
        return result

    def _call(self, method, request) -> dict:
        try:
            return self._dict(
                method(request, metadata=self.metadata, timeout=self.timeout_s)
            )
        except grpc.RpcError as exc:
            unavailable = exc.code() in {
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.CANCELLED,
            }
            message = (
                "Log service is unavailable"
                if unavailable
                else exc.details() or "Log service request failed"
            )
            raise LogClientError(message, unavailable=unavailable) from None

    def list_sources(self) -> dict:
        return self._call(self._stub.ListSources, logs_pb2.ListSourcesRequest())

    def apply_source(
        self,
        name: str,
        provider: str,
        config: dict,
        credentials: dict[str, str],
    ) -> dict:
        values = Struct()
        values.update(config)
        return self._call(
            self._stub.ApplySource,
            logs_pb2.ApplySourceRequest(
                name=name,
                provider=provider,
                config=values,
                credentials=credentials,
            ),
        )

    def delete_source(self, name: str) -> dict:
        return self._call(
            self._stub.DeleteSource,
            logs_pb2.DeleteSourceRequest(name=name),
        )

    def test_source(self, name: str) -> dict:
        return self._call(
            self._stub.TestSource,
            logs_pb2.TestSourceRequest(name=name),
        )

    def discover(self, name: str) -> dict:
        request = Struct()
        request.update({"source": name})
        return self._call(self._stub.Discover, request)

    def fetch(self, source: str, start: float, end: float, limit: int) -> dict:
        return self._call(
            self._stub.Fetch,
            logs_pb2.FetchRequest(
                source=source,
                start_unix_s=start,
                end_unix_s=end,
                limit=limit,
            ),
        )

    def investigate(
        self, source: str, start: float, end: float, target_records: int | None,
        focus: str = "",
    ) -> dict:
        request = logs_pb2.InvestigateRequest(
            source=source,
            start_unix_s=start,
            end_unix_s=end,
            focus=focus,
        )
        if target_records is not None:
            request.target_records = target_records
        return self._call(self._stub.Investigate, request)

    def fetch_complete(self, source: str, start: float, end: float) -> dict:
        return self._call(
            self._stub.FetchComplete,
            logs_pb2.FetchCompleteRequest(
                source=source,
                start_unix_s=start,
                end_unix_s=end,
            ),
        )

    def close(self) -> None:
        self._channel.close()
