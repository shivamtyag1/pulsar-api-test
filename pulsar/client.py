"""Minimal rate-limited GraphQL client for the Pulsar API.

Deliberately small. This is a POC client, not the production extractor -
its job is to let us find out how the API actually behaves before we
commit to a design.
"""

import json
import os
import random
import threading
import time

import httpx

# The four Pulsar v2 endpoints. REST and GraphQL v1 are deprecated -
# do not add them.
ENDPOINTS = {
    "trac_data": "https://data.pulsarplatform.com/graphql/trac",
    "trac_meta": "https://trac.pulsarplatform.com/graphql",
    "core_data": "https://data.pulsarplatform.com/graphql/core",
    "pusher": "https://interaction-pusher.pulsarplatform.com/graphql",
}

# Documented limit is 5 req/s. We sit under it deliberately - the entire
# daily pull finishes in minutes either way, so there is nothing to gain
# from racing the ceiling.
DEFAULT_RATE = 3.0

# The limit applies to the token, not the process. If this POC ever grows
# parallel workers, they must share one limiter or the token gets throttled.
MAX_RETRIES = 5
TIMEOUT_SECONDS = 60


class PulsarError(RuntimeError):
    """Raised when the API returns errors we should not retry past."""


class RateLimiter:
    """Simple thread-safe minimum-interval limiter."""

    def __init__(self, per_second: float = DEFAULT_RATE):
        self._min_interval = 1.0 / per_second
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            sleep_for = self._last + self._min_interval - now
            if sleep_for > 0:
                time.sleep(sleep_for)
                now = time.monotonic()
            self._last = now


class PulsarClient:
    def __init__(self, endpoint: str, rate: float = DEFAULT_RATE):
        if endpoint in ENDPOINTS:
            self.url = ENDPOINTS[endpoint]
        else:
            self.url = endpoint

        token = os.environ.get("PULSAR_TOKEN")
        if not token:
            raise SystemExit(
                "PULSAR_TOKEN is not set.\n"
                "Copy the bearer token from Keeper, then:\n"
                "  export PULSAR_TOKEN='...'\n"
                "Never commit it or paste it into a file in this repo."
            )

        self._client = httpx.Client(
            timeout=TIMEOUT_SECONDS,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        self._limiter = RateLimiter(rate)
        self.request_count = 0

    def execute(self, query: str, variables: dict | None = None) -> dict:
        """Run a query and return the `data` block.

        Raises rather than returning partial results - a GraphQL response
        can carry HTTP 200 with an `errors` array, and treating that as
        success is how you silently ingest incomplete data.
        """
        payload = {"query": query, "variables": variables or {}}

        for attempt in range(MAX_RETRIES):
            self._limiter.wait()
            self.request_count += 1

            try:
                resp = self._client.post(self.url, json=payload)
            except httpx.RequestError as exc:
                if attempt == MAX_RETRIES - 1:
                    raise PulsarError(f"network error after retries: {exc}") from exc
                self._backoff(attempt)
                continue

            # A dead token is a human process - renewal goes through the
            # Pulsar account manager. Retrying just delays that phone call.
            if resp.status_code in (401, 403):
                raise PulsarError(
                    f"HTTP {resp.status_code} - token rejected. "
                    "Check it is current in Keeper; renewal is via the account manager."
                )

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                print(f"  429 throttled (Retry-After: {retry_after or 'absent'})")
                self._backoff(attempt, float(retry_after) if retry_after else None)
                continue

            if resp.status_code >= 500:
                print(f"  HTTP {resp.status_code}, retrying")
                self._backoff(attempt)
                continue

            if resp.status_code != 200:
                raise PulsarError(f"HTTP {resp.status_code}: {resp.text[:400]}")

            body = resp.json()

            if body.get("errors"):
                raise PulsarError(
                    "GraphQL errors:\n"
                    + json.dumps(body["errors"], indent=2)[:2000]
                )

            return body.get("data", {})

        raise PulsarError(f"gave up after {MAX_RETRIES} attempts")

    @staticmethod
    def _backoff(attempt: int, retry_after: float | None = None) -> None:
        delay = retry_after if retry_after else (2**attempt) + random.uniform(0, 1)
        time.sleep(delay)

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
