"""Run the complete demonstration using model-free local HTTP processes."""
from __future__ import annotations

import argparse
import hashlib
import json
import socket
import sys

from bench import demo, run_bench


def test_asset_metadata_describes_selected_files(tmp_path):
    binary = tmp_path / "backend"
    binary.write_text(f"#!{sys.executable}\nprint('fake-backend build test')\n")
    binary.chmod(0o755)
    model = tmp_path / "weights"
    model.write_bytes(b"distinct model")
    actual = run_bench.asset_metadata(binary, model)
    assert actual["model_sha256"] == hashlib.sha256(b"distinct model").hexdigest()
    assert actual["binary_sha256"] == hashlib.sha256(binary.read_bytes()).hexdigest()
    assert actual["llama_server_version"] == "fake-backend build test"
    assert "llama_cpp_commit" not in actual


async def test_demo_shows_cache_then_health_failover_and_cleans_up(tmp_path, monkeypatch):
    binary = tmp_path / "fake-backend"
    binary.write_text(f'''#!{sys.executable}
import http.server, json, sys
if '--version' in sys.argv:
    print('fake backend test-build')
    raise SystemExit(0)
requests = 0
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{{"status":"ok"}}')
    def do_POST(self):
        global requests
        self.rfile.read(int(self.headers.get('content-length', 0)))
        requests += 1
        final = {{'stop': True, 'timings': {{'cache_n': 80 if requests > 1 else 0,
                 'prompt_n': 100, 'prompt_ms': 1, 'predicted_n': 1, 'predicted_ms': 1}}}}
        raw = b'data: {{"content":"hello","stop":false}}\\n\\n' + b'data: ' + json.dumps(final).encode() + b'\\n\\n'
        self.send_response(200)
        self.send_header('content-type', 'text/event-stream')
        self.send_header('content-length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)
    def log_message(self, *args): pass
port = int(sys.argv[sys.argv.index('--port') + 1])
http.server.HTTPServer(('127.0.0.1', port), Handler).serve_forever()
''')
    binary.chmod(0o755)
    model = tmp_path / "fake.gguf"
    model.write_bytes(b"fake-model")
    monkeypatch.setattr(run_bench, "LLAMA_SERVER", binary)
    monkeypatch.setattr(run_bench, "MODEL", model)
    monkeypatch.setattr(demo, "LLAMA_SERVER", binary)
    monkeypatch.setattr(demo, "MODEL", model)
    # Keep reservations distinct until all three ports have been selected.
    reservations = [socket.socket() for _ in range(3)]
    try:
        for sock in reservations:
            sock.bind(("127.0.0.1", 0))
        ports = [sock.getsockname()[1] for sock in reservations]
    finally:
        for sock in reservations:
            sock.close()
    args = argparse.Namespace(port_a=ports[0], port_b=ports[1], gateway_port=ports[2],
                              outdir=tmp_path / "results")
    await demo.run(args)
    result = json.loads((args.outdir / "summary.json").read_text())
    assert result["status"] == "passed"
    assert result["cache_reuse_observed"] and result["failover_observed"]
    first, second, third = result["requests"]
    assert first["backend"] == second["backend"] != third["backend"]
    assert second["cache_n"] > 0
    assert result["health_after_removal"]["backends"][first["backend"]]["healthy"] is False
    assert not any(run_bench.port_open(port) for port in ports)
