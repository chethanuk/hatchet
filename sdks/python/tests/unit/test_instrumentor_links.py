"""With ``use_links_instead_of_parent`` enabled, the step-run span starts a new
trace and links to the traceparent carried in the action's metadata instead of
becoming its child."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import get_tracer

from hatchet_sdk.config import OpenTelemetryConfig
from hatchet_sdk.opentelemetry.instrumentor import HatchetInstrumentor

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
SPAN_ID = "b7ad6b7169203331"
TRACEPARENT = f"00-{TRACE_ID}-{SPAN_ID}-01"


def _instrumentor(
    exporter: InMemorySpanExporter, use_links: bool
) -> HatchetInstrumentor:
    inst = object.__new__(HatchetInstrumentor)
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    inst._tracer = get_tracer(__name__, "test", provider)
    inst._config = MagicMock()
    inst._config.otel = OpenTelemetryConfig(use_links_instead_of_parent=use_links)
    return inst


def _action(metadata: dict[str, str]) -> Any:
    action = MagicMock()
    action.action_id = "wf:task"
    action.additional_metadata = metadata
    action.get_otel_attributes.return_value = {}
    return action


async def _noop(_action: Any) -> None:
    return None


def _trace_id(span: ReadableSpan) -> str:
    assert span.context is not None
    return format(span.context.trace_id, "032x")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_links", "metadata", "same_trace", "parent_span_id", "n_links"),
    [
        (True, {"traceparent": TRACEPARENT}, False, None, 1),
        (False, {"traceparent": TRACEPARENT}, True, SPAN_ID, 0),
        (True, {}, False, None, 0),
        (True, {"traceparent": "not-a-traceparent"}, False, None, 0),
        (True, {"traceparent": f"00-{'0' * 32}-{'0' * 16}-01"}, False, None, 0),
    ],
    ids=["links", "parent", "links-no-traceparent", "links-malformed", "links-zero"],
)
async def test_step_run_span_parent_or_link(
    use_links: bool,
    metadata: dict[str, str],
    same_trace: bool,
    parent_span_id: str | None,
    n_links: int,
) -> None:
    exporter = InMemorySpanExporter()
    inst = _instrumentor(exporter, use_links)

    await inst._wrap_handle_start_task_run(_noop, MagicMock(), (_action(metadata),), {})

    (span,) = exporter.get_finished_spans()
    assert (_trace_id(span) == TRACE_ID) is same_trace
    if parent_span_id is None:
        assert span.parent is None
    else:
        assert span.parent is not None
        assert format(span.parent.span_id, "016x") == parent_span_id
    assert len(span.links) == n_links
    if n_links:
        link = span.links[0]
        assert format(link.context.trace_id, "032x") == TRACE_ID
        assert format(link.context.span_id, "016x") == SPAN_ID


@pytest.mark.asyncio
@pytest.mark.parametrize("use_links", [True, False], ids=["links", "parent"])
async def test_durable_wait_for_span_stays_in_step_trace(use_links: bool) -> None:
    exporter = InMemorySpanExporter()
    inst = _instrumentor(exporter, use_links)
    action = _action({"traceparent": TRACEPARENT})
    durable_ctx = MagicMock()
    durable_ctx._action = action
    durable_ctx._task_run_external_id = "ext-1"

    async def fake_wait_for(
        *conditions: Any, label: str | None = None, signal_key: str | None = None
    ) -> dict[str, Any]:
        return {}

    async def task(_action: Any) -> None:
        await inst._wrap_aio_wait_for(fake_wait_for, durable_ctx, (), {})

    await inst._wrap_handle_start_task_run(task, MagicMock(), (action,), {})

    spans = {s.name: s for s in exporter.get_finished_spans()}
    step = spans["hatchet.start_step_run"]
    wait = spans["hatchet.durable.wait_for"]
    assert step.context is not None
    assert wait.parent is not None
    assert _trace_id(wait) == _trace_id(step)
    if use_links:
        # nests under the new root step span
        assert wait.parent.span_id == step.context.span_id
    else:
        # default keeps the old behaviour: a sibling under the traceparent
        assert format(wait.parent.span_id, "016x") == SPAN_ID


def test_use_links_reads_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HATCHET_CLIENT_OPENTELEMETRY_USE_LINKS_INSTEAD_OF_PARENT", "1")
    assert OpenTelemetryConfig().use_links_instead_of_parent is True
