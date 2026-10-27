"""Tests for the batching span processor (TRC-278)."""

from traccia.processors.batch_processor import BatchSpanProcessor
from traccia.tracer import TracerProvider
from traccia.tracer.provider import SpanProcessor


class _CollectingExporter:
    def __init__(self):
        self.events = []

    @property
    def exported_names(self):
        return [name for kind, names in self.events if kind == "export" for name in names]

    def export(self, spans):
        self.events.append(("export", [span.name for span in spans]))
        return True

    def shutdown(self):
        self.events.append(("shutdown", []))


class _FlushDuringEnrichment(SpanProcessor):
    """Drains the batch queue while Span.end() is still running enrichment."""

    def __init__(self, batch):
        self.batch = batch

    def on_end(self, span):
        self.batch.force_flush()


class _PendingSpan:
    def __init__(self, name):
        self.name = name
        self._ended = False


def _batch(exporter):
    # Long delay keeps the background worker out of the way; tests flush explicitly.
    return BatchSpanProcessor(exporter, schedule_delay_millis=60_000)


def test_span_drained_while_ending_is_still_exported():
    provider = TracerProvider()
    tracer = provider.get_tracer("test")
    exporter = _CollectingExporter()
    batch = _batch(exporter)
    provider.add_span_processor(batch)
    provider.add_span_processor(_FlushDuringEnrichment(batch))

    span = tracer.start_span("work")
    span.end()
    batch.shutdown()

    assert exporter.exported_names == ["work"]


def test_unended_span_stays_queued_until_it_ends():
    exporter = _CollectingExporter()
    batch = _batch(exporter)
    span = _PendingSpan("pending")

    batch.on_end(span)
    batch.force_flush()
    assert exporter.exported_names == []

    span._ended = True
    batch.force_flush()
    assert exporter.exported_names == ["pending"]
    batch.shutdown()


def test_unended_span_does_not_block_ended_spans_behind_it():
    exporter = _CollectingExporter()
    batch = _batch(exporter)
    pending = _PendingSpan("pending")
    done = _PendingSpan("done")
    done._ended = True

    batch.on_end(pending)
    batch.on_end(done)
    batch.force_flush()
    assert exporter.exported_names == ["done"]

    pending._ended = True
    batch.force_flush()
    assert exporter.exported_names == ["done", "pending"]
    batch.shutdown()


def test_shutdown_exports_then_shuts_down_exporter_once():
    provider = TracerProvider()
    tracer = provider.get_tracer("test")
    exporter = _CollectingExporter()
    batch = _batch(exporter)
    provider.add_span_processor(batch)

    tracer.start_span("work").end()
    batch.shutdown()
    # stop_tracing() shuts the processor down directly and again via the provider.
    provider.shutdown()

    assert exporter.events == [("export", ["work"]), ("shutdown", [])]
