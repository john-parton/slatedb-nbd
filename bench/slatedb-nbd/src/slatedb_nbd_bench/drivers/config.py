import itertools as it
import logging
from collections.abc import Iterator
from typing import NotRequired, TypedDict

logger = logging.getLogger(__name__)


class _TestConfig(TypedDict):
    driver: str
    compression: str | None
    encryption: bool
    ashift: NotRequired[int | None]
    block_size: NotRequired[
        int | None
    ]  # Really only a small number of values are appropriate
    slog_size: NotRequired[int | None]  # Only used for SlateDB NBD tests
    connections: NotRequired[int | None]  # Number of connections to use for NBD
    wal_enabled: NotRequired[bool | None]  # Whether to enable WAL for SlateDB NBD
    object_store_cache: NotRequired[
        bool | None
    ]  # Whether to enable object store caching for SlateDB NBD


DRIVER_DEFAULTS = {
    "zerofs": {
        "encryption": False,
    },
    "slatedb-nbd": {
        "encryption": True,
        "ashift": 12,
        "block_size": 4096,
    },
}


def get_text_matrix(
    *,
    drivers: list[str],
    compression: list[str],
    connections: list[int],
    wal_enabled: list[bool],
    object_store_cache: list[bool],
) -> Iterator[_TestConfig]:
    conf = it.product(
        drivers,
        compression,
        connections,
        wal_enabled,
        object_store_cache,
    )

    for (
        case_driver,
        case_compression,
        case_connections,
        case_wal_enabled,
        case_object_store_cache,
    ) in conf:
        yield {
            **DRIVER_DEFAULTS.get(case_driver, {}),
            "driver": case_driver,
            "compression": None if case_compression == "off" else case_compression,
            "connections": case_connections,
            "wal_enabled": case_wal_enabled,
            "object_store_cache": case_object_store_cache,
        }
