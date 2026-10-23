"""Rate-limited GraphQL client for the Pulsar API.

Small on purpose. Its error taxonomy is the part that matters and was
learned against the live API:

  * HTTP 200 with a non-empty `errors` array is a failure, not a partial
    success. Treating it as success is how incomplete data reaches a
    dashboard without anyone noticing.
  * 401/403 is never retried. Token renewal is a human process through the
    Pulsar account manager, so retrying only delays that phone call.
  * 429 honours `Retry-After` when the vendor sends it.

The rate limit applies to the *token*, not the process. Parallel workers
must therefore share one limiter - see `shared_limiter`.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import threading
import time
from types import TracebackType
from typing import Any

import httpx

log = logging.getLogger(__name__)

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


def short_error(exc: BaseException, limit: int = 200) -> str:
    """One line for a log: the GraphQL `message`s, not the whole JSON body.

    A vendor error arrives as pretty-printed JSON with trace ids; printed in
    full per dropped series it buries the one sentence that matters.
    """
    text = str(exc)
    messages = re.findall(r'"message":\s*"((?:[^"\\]|\\.)*)"', text)
    line = "; ".join(dict.fromkeys(messages)) if messages else text.splitlines()[0]
    return line if len(line) <= limit else line[: limit - 3] + "..."


class AuthError(PulsarError):
    """The token was rejected (HTTP 401/403).

    Its own type so a caller that isolates per-facet or per-search failures
    can still let this one through: a dead token fails every later call too,
    and "31 facets dropped" is a much worse message than "token rejected".
    """


class MissingTokenError(PulsarError):
    """PULSAR_TOKEN is absent from the environment.

    A subclass rather than SystemExit: in a Lambda, SystemExit surfaces as
    an opaque runtime failure with no usable message.
    """


class RateLimiter:
    """Thread-safe minimum-interval limiter."""

    def __init__(self, per_second: float = DEFAULT_RATE) -> None:
        if per_second <= 0:
            raise ValueError("per_second must be positive")
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


_SHARED_LIMITER: RateLimiter | None = None
_SHARED_LIMITER_LOCK = threading.Lock()


def shared_limiter(per_second: float = DEFAULT_RATE) -> RateLimiter:
    """Return the process-wide limiter.

    The quota belongs to the token, so two clients each granted 3 req/s
    would together exceed the ceiling. Anything running concurrently must
    go through this.
    """
    global _SHARED_LIMITER
    with _SHARED_LIMITER_LOCK:
        if _SHARED_LIMITER is None:
            _SHARED_LIMITER = RateLimiter(per_second)
        return _SHARED_LIMITER


class PulsarClient:
    def __init__(
        self,
        endpoint: str,
        rate: float = DEFAULT_RATE,
        *,
        limiter: RateLimiter | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.url = ENDPOINTS.get(endpoint, endpoint)

        token = os.environ.get("PULSAR_TOKEN")
        if not token:
            raise MissingTokenError(
                "PULSAR_TOKEN is not set.\n"
                "Copy the bearer token from Keeper, then:\n"
                "  export PULSAR_TOKEN='...'\n"
                "Never commit it or paste it into a file in this repo."
            )

        self._client = httpx.Client(
            timeout=TIMEOUT_SECONDS,
            transport=transport,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        self._limiter = limiter or RateLimiter(rate)
        self.request_count = 0

    def execute(
        self, query: str, variables: dict[str, object] | None = None
    ) -> dict[str, Any]:
        """Run a query and return the `data` block.

        The return is `dict[str, Any]` rather than `dict[str, object]`
        deliberately. A GraphQL `data` block is vendor-shaped JSON with no
        static type available to us, so `object` would force a cast at every
        call site while adding no safety - the real guarantee is the runtime
        `isinstance` check below, which stays.

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
                raise AuthError(
                    f"HTTP {resp.status_code} - token rejected. "
                    "Check it is current in Keeper; renewal is via the account manager."
                )

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                log.warning(
                    "throttled: HTTP 429 (Retry-After: %s)", retry_after or "absent"
                )
                self._backoff(attempt, _as_float(retry_after))
                continue

            if resp.status_code >= 500:
                log.warning("HTTP %s from vendor, retrying", resp.status_code)
                self._backoff(attempt)
                continue

            if resp.status_code != 200:
                raise PulsarError(f"HTTP {resp.status_code}: {resp.text[:400]}")

            body: dict[str, Any] = resp.json()

            if body.get("errors"):
                raise PulsarError(
                    "GraphQL errors:\n" + json.dumps(body["errors"], indent=2)[:2000]
                )

            data = body.get("data") or {}
            if not isinstance(data, dict):
                raise PulsarError(f"expected `data` to be an object, got {type(data)}")
            return data

        raise PulsarError(f"gave up after {MAX_RETRIES} attempts")

    @staticmethod
    def _backoff(attempt: int, retry_after: float | None = None) -> None:
        # nosec B311 - jitter, not cryptography
        delay = retry_after if retry_after else (2**attempt) + random.uniform(0, 1)  # noqa: S311
        time.sleep(delay)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> PulsarClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


def _as_float(value: str | None) -> float | None:
    """Parse a Retry-After header defensively.

    The header is vendor-controlled and may be absent, malformed, or an
    HTTP-date rather than seconds. A bad parse must not mask the 429.
    """
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        log.warning("unparseable Retry-After header: %r", value)
        return None
