from typing import Generator, Optional

from fastapi import HTTPException, Request
from httpx import AsyncClient, Timeout
from jose import jwt

from src.core.config import settings
from src.db.session import session_manager

http_client: Optional[AsyncClient] = None


async def get_db() -> Generator:
    async with session_manager.session() as session:
        yield session

def get_user_id(request: Request):
    """Get the user ID from the JWT token or use the pre-defined user_id if running without authentication."""
    # Check if the request has an Authorization header
    authorization = request.headers.get("Authorization")

    if authorization:
        # Split the Authorization header into the scheme and the token
        scheme, _, token = authorization.partition(" ")

        if scheme.lower() != "bearer":
            raise HTTPException(status_code=401, detail="Invalid Authorization Scheme")
        if not token:
            raise HTTPException(status_code=401, detail="Missing Authorization Token")

        # Decode the JWT token and extract the user_id
        return jwt.get_unverified_claims(token)["sub"]

    else:
        # This is returned if there is no Authorization header and therefore no authentication.
        scheme, _, token = settings.SAMPLE_AUTHORIZATION.partition(" ")
        return jwt.get_unverified_claims(token)["sub"]


def get_http_client():
    """Returns an asynchronous HTTP client, typically used for connecting to the GOAT Routing service."""

    global http_client
    if http_client is None:
        http_client = AsyncClient(
            timeout=Timeout(
                settings.ASYNC_CLIENT_DEFAULT_TIMEOUT,
                read=settings.ASYNC_CLIENT_READ_TIMEOUT,
            )
        )
    return http_client


async def close_http_client():
    """Clean-up network resources used by the HTTP client."""

    global http_client
    if http_client is not None:
        await http_client.aclose()
        http_client = None
