"""Keep pytest from opening exporter connections."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def block_otlp_export(monkeypatch):
    from opentelemetry.sdk.trace.export import SpanExportResult

    def _export(self, spans):  # noqa: ARG001
        return SpanExportResult.SUCCESS

    monkeypatch.setattr(
        "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter.export",
        _export,
        raising=False,
    )
    monkeypatch.setattr("traccia.exporter.otlp_exporter.OTLPExporter.export", lambda self, spans: True)
