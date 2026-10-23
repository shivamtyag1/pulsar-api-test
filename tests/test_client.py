"""Client error taxonomy.

These behaviours were learned against the live API and are the difference
between "the pipeline failed" and "the pipeline quietly loaded half the
data". They are tested against a mock transport so they cost no quota.
"""

from __future__ import annotations

import httpx
import pytest

from pulsar.client import (
    ENDPOINTS,
    MissingTokenError,
    PulsarClient,
    PulsarError,
    RateLimiter,
    _as_float,
    shared_limiter,
)

TOKEN_ENV = "PULSAR_TOKEN"


@pytest.fixture(autouse=True)
def _token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(TOKEN_ENV, "test-token-not-a-real-secret")


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Backoff and rate limiting must not make the suite slow."""
    monkeypatch.setattr("pulsar.client.time.sleep", lambda _: None)


def client_returning(*responses: httpx.Response) -> PulsarClient:
    """A client whose transport replays the given responses in order."""
    queue = list(responses)

    def handler(_request: httpx.Request) -> httpx.Response:
        return queue.pop(0) if queue else responses[-1]

    return PulsarClient("trac_data", transport=httpx.MockTransport(handler))


class TestGraphQLErrorHandling:
    def test_http_200_with_errors_array_raises(self) -> None:
        """The trap: GraphQL reports failure with a 200 status code.

        Returning `data` here would silently ingest incomplete results.
        """
        body = {
            "data": {"results": {"total": 5, "results": []}},
            "errors": [{"message": "Field 'bogus' doesn't exist"}],
        }

        with (
            client_returning(httpx.Response(200, json=body)) as client,
            pytest.raises(PulsarError, match="GraphQL errors"),
        ):
            client.execute("query { bogus }")

    def test_successful_response_returns_data_block_only(self) -> None:
        body = {"data": {"results": {"total": 42}}}

        with client_returning(httpx.Response(200, json=body)) as client:
            result = client.execute("query { results }")

        assert result == {"results": {"total": 42}}

    def test_null_data_is_not_an_error(self) -> None:
        with client_returning(httpx.Response(200, json={"data": None})) as client:
            assert client.execute("query { x }") == {}

    def test_non_object_data_raises(self) -> None:
        with (
            client_returning(httpx.Response(200, json={"data": [1, 2]})) as client,
            pytest.raises(PulsarError, match="expected `data`"),
        ):
            client.execute("query { x }")


class TestAuthFailures:
    @pytest.mark.parametrize("status", [401, 403])
    def test_auth_failure_raises_immediately_without_retrying(
        self, status: int
    ) -> None:
        """Token renewal is a human process; retrying only delays the call."""
        with client_returning(httpx.Response(status, json={})) as client:
            with pytest.raises(PulsarError, match="token rejected"):
                client.execute("query { x }")

            assert client.request_count == 1, "must not retry a dead token"

    def test_missing_token_raises_a_typed_error_not_system_exit(self) -> None:
        """SystemExit in a Lambda is an opaque failure with no message."""
        import os

        original = os.environ.pop(TOKEN_ENV, None)
        try:
            with pytest.raises(MissingTokenError, match="Keeper"):
                PulsarClient("trac_data")
        finally:
            if original is not None:
                os.environ[TOKEN_ENV] = original

    def test_missing_token_error_is_a_pulsar_error(self) -> None:
        assert issubclass(MissingTokenError, PulsarError)


class TestRetryBehaviour:
    def test_retries_on_429_then_succeeds(self) -> None:
        with client_returning(
            httpx.Response(429, headers={"Retry-After": "1"}),
            httpx.Response(200, json={"data": {"ok": True}}),
        ) as client:
            assert client.execute("query { x }") == {"ok": True}
            assert client.request_count == 2

    def test_retries_on_5xx_then_succeeds(self) -> None:
        with client_returning(
            httpx.Response(503),
            httpx.Response(200, json={"data": {"ok": True}}),
        ) as client:
            assert client.execute("query { x }") == {"ok": True}

    def test_gives_up_after_max_retries(self) -> None:
        with (
            client_returning(httpx.Response(500)) as client,
            pytest.raises(PulsarError, match="gave up"),
        ):
            client.execute("query { x }")

    def test_unexpected_status_raises_without_retrying(self) -> None:
        with client_returning(httpx.Response(418, text="teapot")) as client:
            with pytest.raises(PulsarError, match="418"):
                client.execute("query { x }")
            assert client.request_count == 1

    def test_network_error_retries_then_raises(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused", request=request)

        with PulsarClient("trac_data", transport=httpx.MockTransport(handler)) as c:
            with pytest.raises(PulsarError, match="network error"):
                c.execute("query { x }")
            assert c.request_count > 1, "network errors should be retried"


class TestRetryAfterParsing:
    @pytest.mark.parametrize(
        ("header", "expected"),
        [
            ("30", 30.0),
            ("1.5", 1.5),
            (None, None),
            ("", None),
            # The RFC also permits an HTTP-date, which we cannot use as a
            # float. It must degrade to default backoff, not crash.
            ("Wed, 21 Oct 2026 07:28:00 GMT", None),
            ("garbage", None),
        ],
    )
    def test_parses_defensively(
        self, header: str | None, expected: float | None
    ) -> None:
        assert _as_float(header) == expected


class TestRateLimiting:
    def test_shared_limiter_is_a_singleton(self) -> None:
        """The quota belongs to the token, so concurrent clients must share.

        Two limiters at 3 req/s would together exceed the 5 req/s ceiling.
        """
        assert shared_limiter() is shared_limiter()

    def test_rejects_a_nonsensical_rate(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            RateLimiter(0)

    def test_client_accepts_an_injected_limiter(self) -> None:
        limiter = RateLimiter(100)
        with client_returning(httpx.Response(200, json={"data": {}})) as _:
            pass
        client = PulsarClient(
            "trac_data",
            limiter=limiter,
            transport=httpx.MockTransport(
                lambda _r: httpx.Response(200, json={"data": {}})
            ),
        )
        assert client._limiter is limiter
        client.close()


class TestEndpoints:
    def test_known_endpoint_names_resolve_to_urls(self) -> None:
        client = PulsarClient(
            "trac_meta",
            transport=httpx.MockTransport(lambda _r: httpx.Response(200, json={})),
        )
        assert client.url == ENDPOINTS["trac_meta"]
        client.close()

    def test_unknown_endpoint_is_treated_as_a_literal_url(self) -> None:
        client = PulsarClient(
            "https://example.test/graphql",
            transport=httpx.MockTransport(lambda _r: httpx.Response(200, json={})),
        )
        assert client.url == "https://example.test/graphql"
        client.close()

    def test_all_four_v2_endpoints_are_present(self) -> None:
        assert set(ENDPOINTS) == {"trac_data", "trac_meta", "core_data", "pusher"}
