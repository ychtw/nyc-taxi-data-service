import logging
from threading import Lock
from typing import Awaitable, Callable

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


class GlobalConcurrencyLimiter:
    """
    Limit the number of in-flight API requests within a single process.

    This limiter is intended for local development and single-process
    deployments. It keeps an in-memory count of active requests and rejects
    new requests when the configured concurrency ceiling is reached.
    """

    def __init__(self, max_concurrent_requests: int):
        """
        Initialize the limiter.

        Args:
            max_concurrent_requests: Maximum number of concurrent requests
                allowed at one time.

        Raises:
            ValueError: If the configured limit is not positive.
        """
        if max_concurrent_requests <= 0:
            raise ValueError("max_concurrent_requests must be positive")

        self._max_concurrent_requests = max_concurrent_requests
        self._active_requests = 0
        self._lock = Lock()

    @property
    def max_concurrent_requests(self) -> int:
        """
        Return the configured concurrency ceiling.
        """
        return self._max_concurrent_requests

    @property
    def active_requests(self) -> int:
        """
        Return the current number of in-flight requests.
        """
        with self._lock:
            return self._active_requests

    def try_acquire(self) -> bool:
        """
        Attempt to reserve one concurrency slot.

        Returns:
            True if a slot was acquired and the request may proceed.
            False if the concurrency limit has already been reached.
        """
        with self._lock:
            if self._active_requests >= self._max_concurrent_requests:
                return False

            self._active_requests += 1
            return True

    def release(self) -> None:
        """
        Release one previously acquired concurrency slot.

        This should be called exactly once for every successful acquire, even
        when request handling raises an exception.
        """
        with self._lock:
            if self._active_requests > 0:
                self._active_requests -= 1


def register_rate_limit_middleware(
    app: FastAPI, limiter: GlobalConcurrencyLimiter
) -> None:
    """
    Register request-level concurrency limiting middleware on the FastAPI app.

    Args:
        app: FastAPI application instance.
        limiter: Global concurrency limiter used to gate requests.
    """

    @app.middleware("http")
    async def enforce_global_concurrency_limit(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ):
        """
        Reject requests with HTTP 429 when the concurrency ceiling is reached.
        """
        acquired = limiter.try_acquire()
        if not acquired:
            logger.warning(
                "API request rejected | reason=too many concurrent requests | active_requests=%d | max_concurrent_requests=%d",
                limiter.active_requests,
                limiter.max_concurrent_requests,
            )
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many concurrent requests"},
            )

        try:
            response = await call_next(request)
            return response
        finally:
            limiter.release()
