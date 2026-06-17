import re
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

_TEMPLATE_PATTERN = re.compile(r"{{\s*([a-zA-Z0-9_]+)\s*}}")


def render_template(template: str, values: dict[str, Any]) -> str:
    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        value = values.get(key)
        if value is None:
            return ""
        return str(value)

    return _TEMPLATE_PATTERN.sub(replace, template)


def extract_json_path(payload: dict[str, Any], path: str) -> Any:
    if not path.startswith("$."):
        return None
    current: Any = payload
    for part in path[2:].split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def cents(amount_usd: Decimal) -> int:
    return int((amount_usd * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def conversational_tool_response(state: str) -> str:
    responses = {
        "ACTIVE": "I've started that request. It is being processed now.",
        "CREATE_IN_FLIGHT": "I'm starting that request now.",
        "CANCEL_REQUESTED": "I'm cancelling that request now.",
        "CANCEL_IN_FLIGHT": "I'm cancelling that request now.",
        "CANCELLED": "No problem, I cancelled that request.",
        "CANNOT_CANCEL": "That request can no longer be cancelled.",
        "COMPLETED": "That request is complete.",
        "FAILED": "I could not complete that request.",
        "TIMED_OUT": "That request is taking longer than expected.",
    }
    return responses.get(state, "That request is being processed.")
