import os
from collections.abc import Iterator
from contextlib import contextmanager


@contextmanager
def push_pop_cwd(new_cwd: str) -> Iterator[None]:
    original_cwd = os.getcwd()
    os.chdir(new_cwd)
    try:
        yield
    finally:
        os.chdir(original_cwd)
