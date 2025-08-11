import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TypedDict


class _BenchResult(TypedDict):
    label: str
    elapsed: float


@contextmanager
def bench_print(label: str):
    start = time.perf_counter()
    try:
        yield
    finally:
        end = time.perf_counter()
        elapsed = end - start
        print(f"{label}: {elapsed:.6f} seconds")


@dataclass
class Bencher:
    results: list[_BenchResult] = field(default_factory=list)

    def push(self, *, label: str, elapsed: float) -> None:
        """
        Push a benchmark result to the results list.
        """
        self.results.append({"label": label, "elapsed": elapsed})

    @contextmanager
    def bench(self, label: str) -> Iterator[None]:
        """
        Context manager to benchmark a block of code.
        It prints the elapsed time after the block is executed.
        """
        start = time.perf_counter()
        try:
            yield
        finally:
            end = time.perf_counter()
            elapsed = end - start
            self.push(label=label, elapsed=elapsed)
