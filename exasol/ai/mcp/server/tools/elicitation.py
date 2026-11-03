"""
Elicitation support for the MCP 2026-07-28 protocol.

On this protocol the server cannot send an elicitation request to the client in the
middle of a tool call, so ``ctx.elicit()`` is unavailable. Instead, the tool follows
the guard pattern: it returns an ``InputRequiredResult`` that embeds the elicitation
request. The client collects the user input and calls the tool again, with the same
arguments, adding the elicitation response and the opaque state the tool returned.
The tool then reads them from ``ctx.input_responses`` and ``ctx.request_state``.

Older protocols keep using ``ctx.elicit()``.
"""

import json
from dataclasses import dataclass
from typing import Any

from fastmcp import Context
from fastmcp.server.elicitation import (
    handle_elicit_accept,
    parse_elicit_response_type,
)
from mcp.types import (
    ElicitRequest,
    ElicitRequestFormParams,
    ElicitResult,
    InputRequiredResult,
)
from mcp.types.version import MODERN_PROTOCOL_VERSIONS
from pydantic import BaseModel

ELICITATION_KEY = "confirmation"


@dataclass
class ElicitationInput:
    """
    The user's response to an elicitation, received in a guard pattern round.

    Attributes:
        action:
            The elicitation action - "accept", "decline" or "cancel".
        data:
            The accepted data, validated against the response type. None unless
            the action is "accept".
    """

    action: str
    data: Any


def raise_if_not_accepted(action: str, operation: str) -> None:
    """
    Raises an ``InterruptedError`` if the elicitation action is anything other than
    "accept".

    Args:
        action:
            The elicitation action.
        operation:
            The name of the operation, used in the error message, e.g. "The file
            operation".
    """
    if action == "accept":
        return
    if action == "decline":
        raise InterruptedError(f"{operation} is declined by the user.")
    # cancel
    raise InterruptedError(f"{operation} is cancelled by the user.")


def is_input_required_era(ctx: Context) -> bool:
    """
    Tells whether the client connection uses a protocol, on which the elicitation
    has to follow the guard pattern.
    """
    request_context = ctx.request_context
    if request_context is None:
        return False
    return request_context.protocol_version in MODERN_PROTOCOL_VERSIONS


def request_input(
    message: str,
    response_type: type[BaseModel],
    state: dict[str, Any] | None = None,
) -> InputRequiredResult:
    """
    Creates the result asking the client to elicit the user input.

    Args:
        message:
            The message to be displayed in the elicitation.
        response_type:
            The type of the elicitation data. The schema is built the same way as
            in ``ctx.elicit()``.
        state:
            Optional data the tool needs in the next round. It is passed back to the
            tool as is. The framework seals it, so that it cannot be tampered with.
            The state is sent even if it is empty, since the sealed state binds the
            next round to this request, see ``read_input``.
    """
    schema = parse_elicit_response_type(response_type).schema
    return InputRequiredResult(
        input_requests={
            ELICITATION_KEY: ElicitRequest(
                params=ElicitRequestFormParams(message=message, requested_schema=schema)
            )
        },
        request_state=json.dumps(state or {}),
    )


def read_input(ctx: Context, response_type: type[BaseModel]) -> ElicitationInput | None:
    """
    Reads the user's response to an elicitation requested in the previous round.
    Returns None if there is no response, which is the case in the first round.

    A response that comes without the request state is ignored, so the tool asks
    again. The framework seals the state with the tool name, the tool arguments and,
    if the client is authenticated, the client identity. It rejects the state if any
    of them changes. Hence, requiring the state ensures that the response answers an
    elicitation this server requested, for the same tool call.

    Args:
        ctx:
            MCP context.
        response_type:
            The type of the elicitation data, used to validate the accepted data.
    """
    input_responses = ctx.input_responses
    if (
        not input_responses
        or (ELICITATION_KEY not in input_responses)
        or not ctx.request_state
    ):
        return None
    response = input_responses[ELICITATION_KEY]
    if not isinstance(response, ElicitResult):
        raise TypeError(
            f"Unexpected response to the elicitation request: {type(response)}."
        )
    data = None
    if response.action == "accept":
        config = parse_elicit_response_type(response_type)
        data = handle_elicit_accept(config, response.content).data
    return ElicitationInput(action=response.action, data=data)


def read_request_state(ctx: Context) -> dict[str, Any]:
    """
    Reads the state returned by the tool in the previous round, see
    ``request_input``. Returns an empty dictionary in the first round.
    """
    return json.loads(ctx.request_state) if ctx.request_state else {}
