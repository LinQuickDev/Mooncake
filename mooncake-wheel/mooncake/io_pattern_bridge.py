"""Framework-neutral, non-blocking CFM metric bridges.

The vLLM connector accepts these objects through ``vllm_config``.  SGLang's
HiCache integration can instantiate :class:`SglangHiCacheIoPatternBridge` at
its request-finished and prefix-match hooks without depending on vLLM.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from queue import Empty, Full, Queue
from threading import Event, Thread
from typing import Any


MetricSink = Callable[[Mapping[str, Any]], None]


class BatchedIoPatternBridge:
    """Bounded asynchronous bridge to a CFM metric reporter.

    ``report`` receives complete records (for example, a CFM RPC client
    method).  Back pressure drops metrics instead of delaying inference.
    """

    def __init__(self, report: MetricSink, capacity: int = 4096) -> None:
        self._report = report
        self._queue: Queue[dict[str, Any]] = Queue(maxsize=capacity)
        self._stopping = Event()
        self._worker = Thread(target=self._run, name="io-pattern-cfm",
                              daemon=True)
        self._worker.start()
        self.dropped = 0

    def report_inference_metrics(self, **metrics: Any) -> None:
        try:
            self._queue.put_nowait(dict(metrics))
        except Full:
            self.dropped += 1

    def close(self) -> None:
        self._stopping.set()
        self._worker.join(timeout=1.0)

    def _run(self) -> None:
        while not self._stopping.is_set() or not self._queue.empty():
            try:
                metrics = self._queue.get(timeout=0.1)
            except Empty:
                continue
            try:
                self._report(metrics)
            except Exception:
                # A failed CFM report must remain isolated from inference.
                self.dropped += 1


class SglangHiCacheIoPatternBridge(BatchedIoPatternBridge):
    """Adapter for SGLang HiCache request and prefix-match hooks.

    ``layout`` must be the active ``--hicache-mem-layout`` value, normally
    ``layer_first``, ``page_first`` or ``page_first_direct``.
    """

    def request_finished(self, *, session_id: str, token_count: int,
                         prefix_depth: int, prefix_fanout: int,
                         match_length: int, continuous_prefix_length: int,
                         recompute_cost: float, request_priority: int = 0,
                         layout: str = "layer_first", layout_group: int = 0) -> None:
        self.report_inference_metrics(
            session_id=session_id,
            token_count=token_count,
            prefix_depth=prefix_depth,
            prefix_fanout=prefix_fanout,
            match_length=match_length,
            continuous_prefix_length=continuous_prefix_length,
            recompute_cost=recompute_cost,
            request_priority=request_priority,
            layout=layout,
            layout_group=layout_group,
        )
