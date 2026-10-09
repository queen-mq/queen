"""Locks and semaphores, as leases with a fencing token (``POST /api/v1/locks``)."""

from .locks import Lock, LockResult, Locks, lock_ttl_seconds

__all__ = ["Lock", "LockResult", "Locks", "lock_ttl_seconds"]
