"""Agent-aware inference gateway (Week 1 scope).

This package implements a small, auditable asyncio gateway in front of two
local llama.cpp servers.  It deliberately does NOT implement bounded queues,
retries, failover or cancellation fault injection yet; those are later gates.
"""

__version__ = "0.1.0"
