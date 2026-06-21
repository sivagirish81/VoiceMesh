from dataclasses import dataclass
from typing import cast

from fastapi import Depends, HTTPException, Request, WebSocket, status

from apps.api.db.repository import PostgresRepository
from apps.api.security import hash_session_token


@dataclass(frozen=True)
class AuthContext:
    session_id: str
    user_id: str
    email: str
    name: str
    organization_id: str
    organization_name: str
    role: str

    def payload(self) -> dict[str, object]:
        return {
            "user": {
                "id": self.user_id,
                "email": self.email,
                "name": self.name,
            },
            "organization": {
                "id": self.organization_id,
                "name": self.organization_name,
            },
            "role": self.role,
        }


def _context_from_row(row: dict[str, object]) -> AuthContext:
    return AuthContext(
        session_id=str(row["session_id"]),
        user_id=str(row["user_id"]),
        email=str(row["email"]),
        name=str(row["name"]),
        organization_id=str(row["organization_id"]),
        organization_name=str(row["organization_name"]),
        role=str(row["role"]),
    )


async def get_current_context(request: Request) -> AuthContext:
    settings = request.app.state.settings
    token = request.cookies.get(settings.session_cookie_name)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    repository = cast(PostgresRepository, request.app.state.repository)
    row = await repository.get_session_context(hash_session_token(token))
    if not row:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired")
    return _context_from_row(row)


async def optional_current_context(request: Request) -> AuthContext | None:
    try:
        return await get_current_context(request)
    except HTTPException:
        return None


async def authenticate_websocket(websocket: WebSocket) -> AuthContext | None:
    settings = websocket.app.state.settings
    token = websocket.cookies.get(settings.session_cookie_name)
    if not token:
        return None
    repository = cast(PostgresRepository, websocket.app.state.repository)
    row = await repository.get_session_context(hash_session_token(token))
    return _context_from_row(row) if row else None


CurrentContext = Depends(get_current_context)
