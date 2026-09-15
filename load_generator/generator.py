"""
generator.py — Load Generator for web_chat assignment
======================================================
Uses the required API routes:
  POST /message  — send a message (fields: client-name, msg)
  GET  /feed     — read all messages

Supports:
  - Variable number of concurrent users (--concurrency)
  - Random / variable message lengths (--min-len / --max-len)
  - Random / variable time intervals (--min-delay / --max-delay)
"""

import sys
import os
import time
import json
import random
import string
import argparse
import logging
import threading
import urllib.request
import urllib.error
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Optional, List

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from load_generator.metrics import MetricsCollector, RequestResult
except ImportError:
    try:
        from metrics import MetricsCollector, RequestResult
    except ImportError:
        from src.load_generator.metrics import MetricsCollector, RequestResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [LoadGen] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("LoadGenerator")

# Simulated users
USERS = [
    "Alice", "Bob", "Charlie", "Diana", "Evan", "Fiona",
    "George", "Hannah", "Ivan", "Julia", "Kevin", "Laura",
    "Mike", "Nina", "Oscar", "Priya", "Quinn", "Rachel",
    "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier",
]

WORDS = [
    "hello", "world", "distributed", "systems", "load", "balancer",
    "performance", "latency", "throughput", "backend", "server",
    "network", "message", "chat", "secure", "encryption", "test",
    "benchmark", "request", "response", "health", "check", "node",
    "cluster", "failover", "retry", "timeout", "queue", "cache",
]


def random_message(min_len: int = 10, max_len: int = 200) -> str:
    """Generate a random message of random length between min_len and max_len chars."""
    length = random.randint(min_len, max_len)
    words = []
    total = 0
    while total < length:
        w = random.choice(WORDS)
        words.append(w)
        total += len(w) + 1
    return " ".join(words)[:length]


class LoadGenerator:
    def __init__(
        self,
        target_url: str,
        total_requests: int = 300,
        concurrency: int = 15,
        scenario: str = "mixed",
        min_msg_len: int = 10,
        max_msg_len: int = 200,
        min_delay_ms: float = 0.0,
        max_delay_ms: float = 100.0,
        timeout: float = 10.0,
        experiment_name: str = "Benchmark"
    ):
        self.target_url = target_url.rstrip("/")
        self.total_requests = max(1, total_requests)
        self.concurrency = max(1, concurrency)
        self.scenario = scenario
        self.min_msg_len = min_msg_len
        self.max_msg_len = max_msg_len
        self.min_delay_ms = min_delay_ms
        self.max_delay_ms = max_delay_ms
        self.timeout = timeout
        self.metrics = MetricsCollector(experiment_name=experiment_name)
        self._completed_count = 0
        self._lock = threading.Lock()

    def _random_delay(self):
        if self.max_delay_ms > 0:
            delay = random.uniform(self.min_delay_ms, self.max_delay_ms) / 1000.0
            if delay > 0:
                time.sleep(delay)

    def _build_post_message(self, req_index: int) -> Dict[str, Any]:
        """Build a POST /message request with random user and message."""
        user = random.choice(USERS)
        msg = random_message(self.min_msg_len, self.max_msg_len)
        body = urllib.parse.urlencode({
            "client-name": user,
            "msg": msg,
        }).encode("utf-8")
        return {
            "method": "POST",
            "path": "/message",
            "headers": {"Content-Type": "application/x-www-form-urlencoded"},
            "body": body,
        }

    def _build_get_feed(self) -> Dict[str, Any]:
        return {
            "method": "GET",
            "path": "/feed",
            "headers": {},
            "body": None,
        }

    def _generate_request_data(self, req_index: int) -> Dict[str, Any]:
        if self.scenario == "write":
            return self._build_post_message(req_index)
        elif self.scenario == "read":
            return self._build_get_feed()
        else:
            # mixed: 60% write, 40% read
            if random.random() < 0.60:
                return self._build_post_message(req_index)
            else:
                return self._build_get_feed()

    def _execute_single_request(self, req_index: int) -> RequestResult:
        self._random_delay()
        req_spec = self._generate_request_data(req_index)
        url = f"{self.target_url}{req_spec['path']}"
        t0 = time.time()

        try:
            req = urllib.request.Request(
                url=url,
                data=req_spec["body"],
                headers=req_spec["headers"],
                method=req_spec["method"]
            )
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                latency_ms = (time.time() - t0) * 1000.0
                status_code = resp.status
                headers = dict(resp.headers)
                backend_id = (
                    headers.get("X-Selected-Backend")
                    or headers.get("X-Backend-Server")
                    or headers.get("X-Handled-By")
                    or "Unknown"
                )
                return RequestResult(
                    success=(200 <= status_code < 400),
                    status_code=status_code,
                    latency_ms=latency_ms,
                    backend_id=backend_id
                )
        except urllib.error.HTTPError as e:
            latency_ms = (time.time() - t0) * 1000.0
            headers = dict(e.headers) if hasattr(e, "headers") else {}
            backend_id = headers.get("X-Selected-Backend") or "Unknown"
            return RequestResult(
                success=False,
                status_code=e.code,
                latency_ms=latency_ms,
                backend_id=backend_id,
                error_message=str(e)
            )
        except Exception as e:
            latency_ms = (time.time() - t0) * 1000.0
            return RequestResult(
                success=False,
                status_code=0,
                latency_ms=latency_ms,
                backend_id="Unavailable",
                error_message=str(e)
            )

    def run(self) -> Dict[str, Any]:
        logger.info(f"Starting load test → {self.target_url}")
        logger.info(
            f"Requests: {self.total_requests} | Concurrency: {self.concurrency} | "
            f"Scenario: {self.scenario} | msg_len: [{self.min_msg_len},{self.max_msg_len}] | "
            f"delay: [{self.min_delay_ms:.0f},{self.max_delay_ms:.0f}]ms"
        )

        self.metrics.start()
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = [
                executor.submit(self._execute_single_request, i + 1)
                for i in range(self.total_requests)
            ]

            for future in as_completed(futures):
                result = future.result()
                self.metrics.record(result)
                with self._lock:
                    self._completed_count += 1
                    step = max(1, self.total_requests // 10)
                    if self._completed_count % step == 0 or self._completed_count == self.total_requests:
                        elapsed = time.time() - start_time
                        rps = self._completed_count / max(0.001, elapsed)
                        pct = (self._completed_count / self.total_requests) * 100
                        print(
                            f" Progress: {self._completed_count}/{self.total_requests} "
                            f"({pct:.0f}%) | {rps:.1f} RPS",
                            end="\r", flush=True
                        )

        self.metrics.finish()
        print()
        self.metrics.print_summary()
        return self.metrics.calculate_summary()


def run_benchmark(
    target_url: str = "http://10.1.75.53:5205",
    requests: int = 300,
    concurrency: int = 15,
    scenario: str = "mixed",
    min_msg_len: int = 10,
    max_msg_len: int = 200,
    min_delay_ms: float = 0.0,
    max_delay_ms: float = 100.0,
    output_file: Optional[str] = None,
    experiment_name: str = "Benchmark"
) -> Dict[str, Any]:
    generator = LoadGenerator(
        target_url=target_url,
        total_requests=requests,
        concurrency=concurrency,
        scenario=scenario,
        min_msg_len=min_msg_len,
        max_msg_len=max_msg_len,
        min_delay_ms=min_delay_ms,
        max_delay_ms=max_delay_ms,
        experiment_name=experiment_name
    )
    results = generator.run()

    if output_file:
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        generator.metrics.save_json(output_file)
        logger.info(f"Metrics saved to {output_file}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Generator for web_chat — uses /message and /feed routes")
    parser.add_argument("--url", type=str, default="http://10.1.75.53:5205",
                        help="Target LB URL (default: http://10.1.75.53:5205)")
    parser.add_argument("--requests", type=int, default=300,
                        help="Total number of requests (default: 300)")
    parser.add_argument("--concurrency", type=int, default=15,
                        help="Number of concurrent users (default: 15)")
    parser.add_argument("--scenario", type=str, default="mixed",
                        choices=["mixed", "write", "read"],
                        help="Workload scenario: mixed (60%% write/40%% read), write, read")
    parser.add_argument("--min-len", type=int, default=10,
                        help="Min message length in chars (default: 10)")
    parser.add_argument("--max-len", type=int, default=200,
                        help="Max message length in chars (default: 200)")
    parser.add_argument("--min-delay", type=float, default=0.0,
                        help="Min inter-request delay in ms per user (default: 0)")
    parser.add_argument("--max-delay", type=float, default=100.0,
                        help="Max inter-request delay in ms per user (default: 100)")
    parser.add_argument("--output", type=str, default=None,
                        help="Path to save JSON metrics")
    parser.add_argument("--name", type=str, default="Benchmark",
                        help="Experiment name")

    args = parser.parse_args()
    run_benchmark(
        target_url=args.url,
        requests=args.requests,
        concurrency=args.concurrency,
        scenario=args.scenario,
        min_msg_len=args.min_len,
        max_msg_len=args.max_len,
        min_delay_ms=args.min_delay,
        max_delay_ms=args.max_delay,
        output_file=args.output,
        experiment_name=args.name
    )
