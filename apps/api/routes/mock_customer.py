import asyncio
from typing import Any

from fastapi import APIRouter

router = APIRouter(prefix="/mock-customer", tags=["mock-customer"])

_refunds: dict[str, dict[str, Any]] = {}


@router.post("/refund-requests")
async def create_refund_request(body: dict[str, Any]) -> dict[str, Any]:
    delay_seconds = float(body.get("delay_seconds", 3))
    refund_id = str(body.get("refund_request_id", "rr_001"))
    await asyncio.sleep(delay_seconds)
    _refunds[refund_id] = {
        "refund_request_id": refund_id,
        "status": "active",
        "message": "Refund request accepted.",
    }
    return _refunds[refund_id]


@router.post("/refund-requests/{refund_request_id}/cancel")
async def cancel_refund_request(refund_request_id: str) -> dict[str, Any]:
    record = _refunds.setdefault(
        refund_request_id,
        {
            "refund_request_id": refund_request_id,
            "status": "active",
            "message": "Refund request accepted.",
        },
    )
    if record["status"] in {"refunded", "rejected"}:
        record["status"] = "cannot_cancel"
        record["message"] = "Refund request can no longer be cancelled."
    else:
        record["status"] = "cancelled"
        record["message"] = "Refund request cancelled."
    return record


@router.get("/refund-requests/{refund_request_id}")
async def get_refund_request(refund_request_id: str) -> dict[str, Any]:
    return _refunds.get(
        refund_request_id,
        {
            "refund_request_id": refund_request_id,
            "status": "active",
            "message": "Refund request is still active.",
        },
    )


@router.post("/webhook-sink")
async def webhook_sink(body: dict[str, Any]) -> dict[str, Any]:
    return {"received": True, "keys": sorted(body.keys())}
