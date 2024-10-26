from __future__ import annotations

import contextlib
import dataclasses
import json
import pathlib
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, TextIO

try:
    import ray
except ImportError:
    raise

from daft.execution.execution_step import PartitionTask

if TYPE_CHECKING:
    from daft import ResourceRequest
    from daft.execution.physical_plan import MaterializedPhysicalPlan


# We add the trace by default to the latest session logs of the Ray Runner
DEFAULT_RAY_LOGS_LOCATION = pathlib.Path("/tmp") / "ray" / "session_latest"
DEFAULT_DAFT_TRACE_LOCATION = DEFAULT_RAY_LOGS_LOCATION / "daft"


@contextlib.contextmanager
def ray_tracer(job_id: str, metrics_actor: ray.actor.ActorHandle):
    # Dump the RayRunner trace if we detect an active Ray session, otherwise we give up and do not write the trace
    if pathlib.Path(DEFAULT_RAY_LOGS_LOCATION).exists():
        trace_filename = (
            f"trace_RayRunner.{job_id}.{datetime.replace(datetime.now(), microsecond=0).isoformat()[:-3]}.json"
        )
        daft_trace_location = pathlib.Path(DEFAULT_DAFT_TRACE_LOCATION)
        daft_trace_location.mkdir(exist_ok=True, parents=True)
        filepath = DEFAULT_DAFT_TRACE_LOCATION / trace_filename
    else:
        filepath = None

    tracer_start = time.time()

    if filepath is not None:
        with open(filepath, "w") as f:
            # Initialize the JSON file
            f.write("[")

            # Yield the tracer
            runner_tracer = RunnerTracer(f, tracer_start)
            yield runner_tracer

            # Retrieve metrics from the metrics actor
            metrics = ray.get(metrics_actor.collect.remote())
            for metric in metrics:
                if metric.end is not None:
                    f.write(
                        json.dumps(
                            {
                                "id": metric.task_id,
                                "category": "task",
                                "name": "task_remote_execution",
                                "ph": "b",
                                "pid": 2,
                                "tid": 1,
                                "ts": (metric.start - tracer_start) * 1000 * 1000,
                            }
                        )
                    )
                    f.write(",\n")
                    f.write(
                        json.dumps(
                            {
                                "id": metric.task_id,
                                "category": "task",
                                "name": "task_remote_execution",
                                "ph": "e",
                                "pid": 2,
                                "tid": 1,
                                "ts": (metric.end - tracer_start) * 1000 * 1000,
                            }
                        )
                    )
                    f.write(",\n")
                else:
                    f.write(
                        json.dumps(
                            {
                                "id": metric.task_id,
                                "category": "task",
                                "name": "task_remote_execution_start_no_end",
                                "ph": "n",
                                "pid": 2,
                                "tid": 1,
                                "ts": (metric.start - tracer_start) * 1000 * 1000,
                            }
                        )
                    )

            # Add the final touches to the file
            f.write(
                json.dumps({"name": "process_name", "ph": "M", "pid": 1, "args": {"name": "RayRunner dispatch loop"}})
            )
            f.write(",\n")
            f.write(json.dumps({"name": "process_name", "ph": "M", "pid": 2, "args": {"name": "Ray Task Execution"}}))
            f.write("\n]")
    else:
        runner_tracer = RunnerTracer(None, tracer_start)
        yield runner_tracer


class RunnerTracer:
    def __init__(self, file: TextIO | None, start: float):
        self._file = file
        self._start = start

    def _write_event(self, event: dict[str, Any]):
        if self._file is not None:
            self._file.write(
                json.dumps(
                    {
                        **event,
                        "ts": int((time.time() - self._start) * 1000 * 1000),
                    }
                )
            )
            self._file.write(",\n")

    @contextlib.contextmanager
    def dispatch_wave(self, wave_num: int):
        self._write_event(
            {
                "name": f"wave-{wave_num}",
                "pid": 1,
                "tid": 1,
                "ph": "B",
                "args": {"wave_num": wave_num},
            }
        )

        metrics = {}

        def metrics_updater(**kwargs):
            metrics.update(kwargs)

        yield metrics_updater

        self._write_event(
            {
                "name": f"wave-{wave_num}",
                "pid": 1,
                "tid": 1,
                "ph": "E",
                "args": metrics,
            }
        )

    def count_inflight_tasks(self, count: int):
        self._write_event(
            {
                "name": "dispatch_metrics",
                "ph": "C",
                "pid": 1,
                "tid": 1,
                "args": {"num_inflight_tasks": count},
            }
        )

    ###
    # Tracing the dispatch batching: when the runner is retrieving enough tasks
    # from the physical plan in order to put them into a batch.
    ###

    @contextlib.contextmanager
    def dispatch_batching(self):
        self._write_event(
            {
                "name": "dispatch_batching",
                "pid": 1,
                "tid": 1,
                "ph": "B",
            }
        )
        yield
        self._write_event(
            {
                "name": "dispatch_batching",
                "pid": 1,
                "tid": 1,
                "ph": "E",
            }
        )

    def mark_noop_task_start(self):
        """Marks the start of running a no-op task"""
        self._write_event(
            {
                "name": "no_op_task",
                "pid": 1,
                "tid": 1,
                "ph": "B",
            }
        )

    def mark_noop_task_end(self):
        """Marks the start of running a no-op task"""
        self._write_event(
            {
                "name": "no_op_task",
                "pid": 1,
                "tid": 1,
                "ph": "E",
            }
        )

    def mark_handle_none_task(self):
        """Marks when the underlying physical plan returns None"""
        self._write_event(
            {
                "name": "Physical Plan returned None, needs more progress",
                "ph": "i",
                "pid": 1,
                "tid": 1,
            }
        )

    def mark_handle_materialized_result(self):
        """Marks when the underlying physical plan returns Materialized Result"""
        self._write_event(
            {
                "name": "Physical Plan returned Materialized Result",
                "ph": "i",
                "pid": 1,
                "tid": 1,
            }
        )

    def mark_handle_task_add_to_batch(self):
        """Marks when the underlying physical plan returns a task that we need to add to the dispatch batch"""
        self._write_event(
            {
                "name": "Physical Plan returned Task to add to batch",
                "ph": "i",
                "pid": 1,
                "tid": 1,
            }
        )

    ###
    # Tracing the dispatching of tasks
    ###

    @contextlib.contextmanager
    def dispatching(self):
        self._write_event(
            {
                "name": "dispatching",
                "pid": 1,
                "tid": 1,
                "ph": "B",
            }
        )
        yield
        self._write_event(
            {
                "name": "dispatching",
                "pid": 1,
                "tid": 1,
                "ph": "E",
            }
        )

    def mark_dispatch_task(self):
        self._write_event(
            {
                "name": "dispatch_task",
                "ph": "i",
                "pid": 1,
                "tid": 1,
            }
        )

    def mark_dispatch_actor_task(self):
        self._write_event(
            {
                "name": "dispatch_actor_task",
                "ph": "i",
                "pid": 1,
                "tid": 1,
            }
        )

    ###
    # Tracing the waiting of tasks
    ###

    @contextlib.contextmanager
    def awaiting(self):
        self._write_event(
            {
                "name": "awaiting",
                "pid": 1,
                "tid": 1,
                "ph": "B",
            }
        )
        yield
        self._write_event(
            {
                "name": "awaiting",
                "pid": 1,
                "tid": 1,
                "ph": "E",
            }
        )

    ###
    # Tracing each individual task as an Async Event
    ###

    def task_created(self, task_id: str, stage_id: int, resource_request: ResourceRequest, instructions: str):
        self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": "task_execution",
                "ph": "b",
                "args": {
                    "task_id": task_id,
                    "resource_request": {
                        "num_cpus": resource_request.num_cpus,
                        "num_gpus": resource_request.num_gpus,
                        "memory_bytes": resource_request.memory_bytes,
                    },
                    "stage_id": stage_id,
                    "instructions": instructions,
                },
                "pid": 2,
                "tid": 1,
            }
        )

    def task_dispatched(self, task_id: str):
        self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": "task_dispatch",
                "ph": "b",
                "pid": 2,
                "tid": 1,
            }
        )

    def task_not_ready(self, task_id: str):
        self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": "task_awaited_not_ready",
                "ph": "n",
                "pid": 2,
                "tid": 1,
            }
        )

    def task_received_as_ready(self, task_id: str):
        self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": "task_dispatch",
                "ph": "e",
                "pid": 2,
                "tid": 1,
            }
        )
        self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": "task_execution",
                "ph": "e",
                "pid": 2,
                "tid": 1,
            }
        )


class RayModuleWrapper:
    """Wrapper around the `ray` module that allows us to hook into various methods for tracing"""

    def __init__(self, runner_tracer: RunnerTracer):
        self._runner_tracer = runner_tracer

    def wait(self, task_ids: dict, *args, **kwargs):
        readies, not_readies = ray.wait(*args, **kwargs)

        for ready in readies:
            if ready in task_ids:
                self._runner_tracer.task_received_as_ready(task_ids[ready])
        for not_ready in not_readies:
            if not_ready in task_ids:
                self._runner_tracer.task_not_ready(task_ids[not_ready])

        return readies, not_readies


@dataclasses.dataclass(frozen=True)
class RayFunctionWrapper:
    """Wrapper around a Ray remote function that allows us to intercept calls and record the call for a given task ID"""

    f: ray.remote_function.RemoteFunction

    def with_tracing(self, runner_tracer: RunnerTracer, task: PartitionTask) -> RayRunnableFunctionWrapper:
        return RayRunnableFunctionWrapper(f=self.f, runner_tracer=runner_tracer, task=task)

    def options(self, *args, **kwargs) -> RayFunctionWrapper:
        return dataclasses.replace(self, f=self.f.options(*args, **kwargs))

    @classmethod
    def wrap(cls, f: ray.remote_function.RemoteFunction):
        return cls(f=f)


@dataclasses.dataclass(frozen=True)
class RayRunnableFunctionWrapper:
    """Runnable variant of RayFunctionWrapper that supports `.remote` calls"""

    f: ray.remote_function.RemoteFunction
    runner_tracer: RunnerTracer
    task: PartitionTask

    def options(self, *args, **kwargs) -> RayRunnableFunctionWrapper:
        return dataclasses.replace(self, f=self.f.options(*args, **kwargs))

    def remote(self, *args, **kwargs):
        self.runner_tracer.task_dispatched(self.task.id())
        return self.f.remote(*args, **kwargs)


@dataclasses.dataclass(frozen=True)
class MaterializedPhysicalPlanWrapper:
    """Wrapper around MaterializedPhysicalPlan that hooks into tracing capabilities"""

    plan: MaterializedPhysicalPlan
    runner_tracer: RunnerTracer

    def __next__(self):
        item = next(self.plan)

        if isinstance(item, PartitionTask):
            self.runner_tracer.task_created(
                item.id(),
                item.stage_id,
                item.resource_request,
                "-".join(type(i).__name__ for i in item.instructions),
            )

        return item
