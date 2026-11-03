"""Local asyncio inference gateway with session routing and bounded admission.

Includes request/execution deadlines, safe connection retries, health failover,
and HTTP/SSE cancellation handling for static llama.cpp backends.
"""

__version__ = "0.1.0"
