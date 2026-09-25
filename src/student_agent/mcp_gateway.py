from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx2
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from .contracts import Contracts


class EvidenceGateway:
    def __init__(self, session: ClientSession, contracts: Contracts) -> None:
        self._session = session
        self._contracts = contracts
        self._tool_schemas: dict[str, dict[str, Any]] | None = None

    async def list_tools(self) -> list[str]:
        return sorted(await self.describe_tools())

    async def describe_tools(self) -> dict[str, dict[str, Any]]:
        """Return the advertised input schema for every MCP tool.

        Keeping discovery here avoids coupling the workflow to a particular MCP SDK
        representation (the SDK has used both camelCase and snake_case attributes).
        The public ``list_tools`` method is intentionally retained for compatibility
        with simple/fake gateways used by tests.
        """
        if self._tool_schemas is not None:
            return self._tool_schemas
        response = await self._session.list_tools()
        descriptions: dict[str, dict[str, Any]] = {}
        for tool in response.tools:
            schema = getattr(tool, "inputSchema", None)
            if schema is None:
                schema = getattr(tool, "input_schema", None)
            descriptions[tool.name] = schema if isinstance(schema, dict) else {}
        self._tool_schemas = dict(sorted(descriptions.items()))
        return self._tool_schemas

    async def call(self, tool_name: str, *, case_id: str, **arguments: Any) -> dict[str, Any]:
        schemas = await self.describe_tools()
        if tool_name not in schemas:
            raise ValueError(f"Tool was not advertised by MCP discovery: {tool_name}")
        payload = {"case_id": case_id, **arguments}
        result = await self._session.call_tool(tool_name, arguments=payload)
        is_error = getattr(result, "isError", None)
        if is_error is None:
            is_error = getattr(result, "is_error", False)
        if is_error:
            message = " ".join(
                block.text for block in result.content if getattr(block, "text", None)
            )
            raise RuntimeError(f"MCP tool {tool_name} failed: {message or 'unknown error'}")
        evidence = getattr(result, "structuredContent", None)
        if evidence is None:
            evidence = getattr(result, "structured_content", None)
        if evidence is None:
            text_blocks = [block.text for block in result.content if getattr(block, "text", None)]
            if len(text_blocks) != 1:
                raise ValueError(f"MCP tool {tool_name} did not return one evidence object")
            evidence = json.loads(text_blocks[0])
        self._contracts.validate_evidence(evidence, f"MCP tool {tool_name}")
        return evidence


@asynccontextmanager
async def connect_gateway(
    endpoint: str, team_api_key: str, contracts: Contracts
) -> AsyncIterator[EvidenceGateway]:
    headers = {"Authorization": f"Bearer {team_api_key}"}
    timeout = httpx2.Timeout(300.0, connect=30.0, write=30.0, pool=30.0)
    async with (
        httpx2.AsyncClient(headers=headers, timeout=timeout) as http_client,
        streamable_http_client(endpoint, http_client=http_client) as (read_stream, write_stream),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        yield EvidenceGateway(session, contracts)
