from typing import Annotated, cast

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel

from apps.api.auth import AuthContext, get_current_context
from apps.api.db.repository import PostgresRepository
from apps.api.security import hash_session_token, new_session_token

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    email: str
    password: str


class RegisterRequest(BaseModel):
    email: str
    password: str
    name: str
    organization_name: str


def _auth_payload(user: dict[str, object]) -> dict[str, object]:
    return {
        "user": {
            "id": str(user["id"]),
            "email": user["email"],
            "name": user["name"],
        },
        "organization": {
            "id": str(user["organization_id"]),
            "name": user["organization_name"],
        },
        "role": user["role"],
    }


async def _create_session_cookie(
    user: dict[str, object],
    request: Request,
    response: Response,
) -> None:
    repository = cast(PostgresRepository, request.app.state.repository)
    token = new_session_token()
    await repository.create_session(
        user_id=str(user["id"]),
        organization_id=str(user["organization_id"]),
        token_hash=hash_session_token(token),
        ttl_hours=request.app.state.settings.session_ttl_hours,
    )
    response.set_cookie(
        request.app.state.settings.session_cookie_name,
        token,
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=request.app.state.settings.session_ttl_hours * 3600,
    )


@router.post("/login")
async def login(body: LoginRequest, request: Request, response: Response) -> dict[str, object]:
    repository = cast(PostgresRepository, request.app.state.repository)
    user = await repository.login_user(body.email, body.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )
    await _create_session_cookie(user, request, response)
    return _auth_payload(user)


@router.post("/register")
async def register(
    body: RegisterRequest,
    request: Request,
    response: Response,
) -> dict[str, object]:
    if len(body.password) < 8:
        raise HTTPException(status_code=422, detail="Password must be at least 8 characters")
    repository = cast(PostgresRepository, request.app.state.repository)
    try:
        user = await repository.register_workspace(
            email=body.email,
            password=body.password,
            name=body.name,
            organization_name=body.organization_name,
            settings=request.app.state.settings,
        )
    except ValueError as exc:
        if str(exc) == "email_already_registered":
            raise HTTPException(status_code=409, detail="Email is already registered") from exc
        raise
    await _create_session_cookie(user, request, response)
    return _auth_payload(user)


@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> dict[str, str]:
    token = request.cookies.get(request.app.state.settings.session_cookie_name)
    if token:
        repository = cast(PostgresRepository, request.app.state.repository)
        await repository.delete_session(hash_session_token(token))
    response.delete_cookie(request.app.state.settings.session_cookie_name)
    return {"status": "ok", "user_id": context.user_id}


@router.get("/me")
async def me(context: Annotated[AuthContext, Depends(get_current_context)]) -> dict[str, object]:
    return context.payload()
