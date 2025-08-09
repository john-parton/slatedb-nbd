"""Calculate running mean and variance of a sequence of numbers."""

import math
from dataclasses import dataclass, field
from typing import Self


@dataclass
class RunningStats:
    """Calculate running mean and variance of a sequence of numbers.

    See Donald Knuth's Art of Computer Programming, Vol 2, page 232, 3rd edition
    """

    mean: float | None = None
    S: float = 0
    k: int = 0

    def push(self: Self, x: float) -> None:
        """Add a number to the running statistics.

        Parameters
        ----------
        x : float
            The number to add to the running statistics.
        """
        self.k += 1
        if self.mean is None:
            self.mean = x
        else:
            delta = x - self.mean
            self.mean = self.mean + delta / self.k
            self.S = self.S + delta * (x - self.mean)

    @property
    def variance(self: Self) -> float:
        """Calculate the variance of the running statistics."""
        return self.S / (self.k - 1) if self.k > 1 else 0

    @property
    def standard_deviation(self: Self) -> float:
        """Calculate the standard deviation of the running statistics."""
        return math.sqrt(self.variance)


@dataclass
class RunningGeometricStats:
    inner: RunningStats = field(default_factory=RunningStats)

    def push(self: Self, x: float) -> None:
        """Add a number to the running statistics.

        Parameters
        ----------
        x : float
            The number to add to the running statistics.
        """
        self.inner.push(math.log(x))

    @property
    def mean(self):
        return None if self.inner.mean is None else math.exp(self.inner.mean)

    @property
    def standard_deviation(self: Self) -> float:
        """Calculate the standard deviation of the running statistics."""
        return math.exp(self.inner.standard_deviation)
