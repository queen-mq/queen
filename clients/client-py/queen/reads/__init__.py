"""Reads by offset (``queue.fetch``) and partition discovery (``queue.changed_since``)."""

from .offset_reads import (
    ChangedPartition,
    ChangedPartitions,
    FetchedPartition,
    FetchedRecord,
    fetch_partitions,
    partitions_changed_since,
)

__all__ = [
    "ChangedPartition",
    "ChangedPartitions",
    "FetchedPartition",
    "FetchedRecord",
    "fetch_partitions",
    "partitions_changed_since",
]
