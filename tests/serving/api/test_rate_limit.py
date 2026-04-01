import asyncio
from threading import Thread

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from nyc_taxi_data_service.serving.api.rate_limit import (
    GlobalConcurrencyLimiter,
    register_rate_limit_middleware,
)

# ============================================================
# GlobalConcurrencyLimiter tests
# ============================================================


def test_limiter_initial_state() -> None:
    """
    Initialize limiter with valid configuration and verify initial state.
    """
    limiter = GlobalConcurrencyLimiter(max_concurrent_requests=2)

    assert limiter.max_concurrent_requests == 2
    assert limiter.active_requests == 0


def test_limiter_raises_when_max_concurrent_requests_not_positive() -> None:
    """
    Raise ValueError when max_concurrent_requests is zero or negative.
    """
    with pytest.raises(ValueError):
        GlobalConcurrencyLimiter(max_concurrent_requests=0)

    with pytest.raises(ValueError):
        GlobalConcurrencyLimiter(max_concurrent_requests=-1)


def test_try_acquire_succeeds_until_limit_reached() -> None:
    """
    Allow acquiring slots until the concurrency limit is reached.
    """
    limiter = GlobalConcurrencyLimiter(max_concurrent_requests=2)

    assert limiter.try_acquire() is True
    assert limiter.try_acquire() is True
    assert limiter.active_requests == 2


def test_try_acquire_returns_false_when_limit_exceeded() -> None:
    """
    Reject acquisition when concurrency limit has already been reached.
    """
    limiter = GlobalConcurrencyLimiter(max_concurrent_requests=2)

    limiter.try_acquire()
    limiter.try_acquire()

    assert limiter.try_acquire() is False
    assert limiter.active_requests == 2


def test_release_frees_capacity() -> None:
    """
    Release should reduce active request count and allow new acquisitions.
    """
    limiter = GlobalConcurrencyLimiter(max_concurrent_requests=1)

    assert limiter.try_acquire() is True
    assert limiter.active_requests == 1

    limiter.release()

    assert limiter.active_requests == 0
    assert limiter.try_acquire() is True


def test_release_does_not_go_below_zero() -> None:
    """
    Release should not reduce active_requests below zero.
    """
    limiter = GlobalConcurrencyLimiter(max_concurrent_requests=1)

    limiter.release()
    limiter.release()

    assert limiter.active_requests == 0


# ============================================================
# Middleware tests
# ============================================================


@pytest.fixture
def test_app():
    """
    Create a minimal FastAPI app with rate limiting middleware.
    """
    app = FastAPI()

    limiter = GlobalConcurrencyLimiter(max_concurrent_requests=1)
    register_rate_limit_middleware(app, limiter)

    @app.get("/ping")
    async def ping():
        return {"message": "ok"}

    return app


def test_middleware_allows_request_under_limit(test_app: FastAPI) -> None:
    """
    Allow request when concurrency limit is not reached.
    """
    client = TestClient(test_app)

    response = client.get("/ping")

    assert response.status_code == 200
    assert response.json() == {"message": "ok"}


def test_middleware_rejects_when_limit_exceeded() -> None:
    """
    Reject request with HTTP 429 when concurrency limit is exceeded.
    """
    app = FastAPI()

    limiter = GlobalConcurrencyLimiter(max_concurrent_requests=1)
    register_rate_limit_middleware(app, limiter)

    # endpoint that blocks to simulate long-running request
    @app.get("/slow")
    async def slow():
        await asyncio.sleep(0.2)
        return {"message": "done"}

    client = TestClient(app)

    responses = []

    def make_request():
        responses.append(client.get("/slow"))

    # start first request (occupies the slot)
    t1 = Thread(target=make_request)
    t1.start()

    # small delay to ensure first request acquires slot
    asyncio.run(asyncio.sleep(0.05))

    # second request should be rejected
    r2 = client.get("/slow")

    t1.join()

    assert r2.status_code == 429
    assert r2.json() == {"detail": "Too many concurrent requests"}


def test_middleware_releases_slot_after_request() -> None:
    """
    Ensure that concurrency slot is released after request completes.
    """
    app = FastAPI()

    limiter = GlobalConcurrencyLimiter(max_concurrent_requests=1)
    register_rate_limit_middleware(app, limiter)

    @app.get("/ping")
    async def ping():
        return {"message": "ok"}

    client = TestClient(app)

    # first request
    r1 = client.get("/ping")
    assert r1.status_code == 200

    # second request should still succeed (slot released)
    r2 = client.get("/ping")
    assert r2.status_code == 200


def test_middleware_releases_slot_on_exception() -> None:
    """
    Ensure slot is released even when endpoint raises an exception.
    """
    app = FastAPI()

    limiter = GlobalConcurrencyLimiter(max_concurrent_requests=1)
    register_rate_limit_middleware(app, limiter)

    @app.get("/error")
    async def error():
        raise RuntimeError("boom")

    client = TestClient(app)

    # first request raises exception
    with pytest.raises(RuntimeError):
        client.get("/error")

    # slot should already be released after the failed request
    assert limiter.active_requests == 0

    # second request should also reach the endpoint rather than being blocked
    with pytest.raises(RuntimeError):
        client.get("/error")

    # slot should again be released after exception propagation
    assert limiter.active_requests == 0
