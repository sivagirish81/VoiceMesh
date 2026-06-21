from typing import Annotated, Any, Literal, cast

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from apps.api.auth import AuthContext, get_current_context
from apps.api.db.repository import PostgresRepository

router = APIRouter(prefix="/agents", tags=["agents"])


ProviderName = Literal["openai"]
AgentStatus = Literal["active", "paused"]


class AgentCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    description: str = ""
    status: AgentStatus = "active"
    system_prompt: str = Field(min_length=1)
    context_prompt: str = ""
    first_message: str = ""
    stt_provider: ProviderName = "openai"
    stt_model: str = "gpt-realtime-whisper"
    llm_provider: ProviderName = "openai"
    llm_model: str = "gpt-4.1-mini"
    tts_provider: ProviderName = "openai"
    tts_model: str = "gpt-4o-mini-tts"
    tts_voice: str = "alloy"
    tuning: dict[str, Any] = Field(default_factory=dict)


class AgentUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None = None
    status: AgentStatus | None = None
    system_prompt: str | None = Field(default=None, min_length=1)
    context_prompt: str | None = None
    first_message: str | None = None
    stt_provider: ProviderName | None = None
    stt_model: str | None = None
    llm_provider: ProviderName | None = None
    llm_model: str | None = None
    tts_provider: ProviderName | None = None
    tts_model: str | None = None
    tts_voice: str | None = None
    tuning: dict[str, Any] | None = None


@router.get("")
async def list_agents(
    request: Request,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> list[dict[str, Any]]:
    repository = cast(PostgresRepository, request.app.state.repository)
    return await repository.list_agents(context.organization_id)


@router.post("")
async def create_agent(
    body: AgentCreate,
    request: Request,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> dict[str, Any]:
    repository = cast(PostgresRepository, request.app.state.repository)
    return await repository.create_agent(context.organization_id, body.model_dump())


@router.get("/{agent_id}")
async def get_agent(
    agent_id: str,
    request: Request,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> dict[str, Any]:
    repository = cast(PostgresRepository, request.app.state.repository)
    agent = await repository.get_agent(context.organization_id, agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent


@router.patch("/{agent_id}")
async def update_agent(
    agent_id: str,
    body: AgentUpdate,
    request: Request,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> dict[str, Any]:
    repository = cast(PostgresRepository, request.app.state.repository)
    agent = await repository.update_agent(
        context.organization_id,
        agent_id,
        body.model_dump(exclude_unset=True),
    )
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent


@router.get("/{agent_id}/calls")
async def list_agent_calls(
    agent_id: str,
    request: Request,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> list[dict[str, Any]]:
    repository = cast(PostgresRepository, request.app.state.repository)
    agent = await repository.get_agent(context.organization_id, agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return await repository.list_agent_calls(context.organization_id, agent_id)


@router.get("/{agent_id}/calls/{call_id}")
async def get_agent_call(
    agent_id: str,
    call_id: str,
    request: Request,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> dict[str, Any]:
    repository = cast(PostgresRepository, request.app.state.repository)
    call = await repository.get_call(call_id, context.organization_id)
    if not call or str(call.get("agent_id")) != agent_id:
        raise HTTPException(status_code=404, detail="Call not found")
    return call
