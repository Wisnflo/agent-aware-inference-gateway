"""Local asyncio inference gateway with session/prefix routing and admission.

Includes request/execution deadlines, bounded admission, safe connection
retries, health failover, prefix/KV-cache-aware routing, runtime backend
registration and HTTP/SSE cancellation handling for llama.cpp backends.
"""

__version__ = "0.1.0"
