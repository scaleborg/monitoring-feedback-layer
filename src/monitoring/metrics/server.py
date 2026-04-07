"""Prometheus metrics HTTP server."""

from prometheus_client import start_http_server


def serve_metrics(port: int = 8000) -> None:
    """Start a blocking Prometheus metrics HTTP server on the given port."""
    start_http_server(port)
    print(f"Serving metrics on http://0.0.0.0:{port}/metrics")
    # Block forever — this is intended to be the main process loop.
    import threading
    threading.Event().wait()
