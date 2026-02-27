#!/usr/bin/env python3
"""
Mock HTTP server for testing the RisingWave fetcher source connector.

Provides several endpoints that simulate REST API patterns:
  /basic           - Simple JSON array of objects
  /nested          - JSON with nested data path (/data/items)
  /mapped          - JSON with fields that need renaming via field.mappings
  /paged_offset    - Offset/limit paginated API
  /paged_cursor    - Cursor-based paginated API
  /health          - Health check endpoint

Usage:
  python3 mock_server.py [port]
  Default port: 18386
"""

import json
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 18386

# -- Static test data --

BASIC_DATA = [
    {"id": 1, "name": "Alice", "score": 95.5},
    {"id": 2, "name": "Bob", "score": 87.0},
    {"id": 3, "name": "Charlie", "score": 72.3},
]

NESTED_DATA = {
    "status": "ok",
    "data": {
        "items": [
            {"id": 10, "value": "hello"},
            {"id": 20, "value": "world"},
        ]
    },
}

MAPPED_DATA = [
    {"user_id": 100, "user_name": "Dana", "created_at": "2024-01-15"},
    {"user_id": 200, "user_name": "Eve", "created_at": "2024-06-20"},
]

# 10 items total for pagination tests
PAGED_ITEMS = [{"id": i, "val": f"item_{i}"} for i in range(1, 11)]


class FetcherMockHandler(BaseHTTPRequestHandler):
    """Handle GET requests for various test endpoints."""

    def log_message(self, format, *args):
        # Suppress default request log to keep test output clean.
        pass

    def _send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        qs = parse_qs(parsed.query)

        if path == "/health":
            self._send_json({"status": "ok"})

        elif path == "/basic":
            self._send_json(BASIC_DATA)

        elif path == "/nested":
            self._send_json(NESTED_DATA)

        elif path == "/mapped":
            self._send_json(MAPPED_DATA)

        elif path == "/paged_offset":
            offset = int(qs.get("offset", ["0"])[0])
            limit = int(qs.get("limit", ["3"])[0])
            page = PAGED_ITEMS[offset : offset + limit]
            self._send_json(page)

        elif path == "/paged_cursor":
            cursor = qs.get("cursor", [None])[0]
            page_size = 3
            if cursor is None:
                start = 0
            else:
                start = int(cursor)
            page = PAGED_ITEMS[start : start + page_size]
            next_start = start + page_size
            next_cursor = str(next_start) if next_start < len(PAGED_ITEMS) else None
            self._send_json({
                "results": page,
                "meta": {"next_cursor": next_cursor},
            })

        else:
            self._send_json({"error": "not found"}, status=404)


def main():
    server = HTTPServer(("0.0.0.0", PORT), FetcherMockHandler)
    print(f"Fetcher mock server listening on port {PORT}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()


if __name__ == "__main__":
    main()
