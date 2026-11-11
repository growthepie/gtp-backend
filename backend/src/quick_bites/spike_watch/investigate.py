"""Run the Claude agent that explains a spike and drafts a quick bite."""
import asyncio
from dataclasses import dataclass, field

from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, SystemMessage, query

from src.quick_bites.spike_watch.prompt import QUICK_BITE_SCHEMA, SYSTEM_PROMPT, task_prompt
from src.quick_bites.spike_watch.tools import InvestigationTools

MODEL = "claude-opus-5-5"
GTP_MCP_URL = "https://mcp.growthepie.com/mcp"
MAX_TURNS = 80
MAX_BUDGET_USD = 15.0


@dataclass
class Investigation:
    spec: dict | None
    charts: dict
    cost_usd: float
    num_turns: int
    error: str | None = None
    failed_mcp_servers: list = field(default_factory=list)


async def _run(prompt: str, tools: InvestigationTools) -> Investigation:
    options = ClaudeAgentOptions(
        model=MODEL,
        system_prompt=SYSTEM_PROMPT,
        mcp_servers={
            "growthepie": {"type": "http", "url": GTP_MCP_URL},
            "gtp_backend": tools.server(),
        },
        strict_mcp_config=True,
        setting_sources=[],  # don't load CLAUDE.md, .mcp.json or user settings from the Airflow host
        tools=["WebSearch", "WebFetch"],
        allowed_tools=["WebSearch", "WebFetch", "mcp__growthepie__*", "mcp__gtp_backend__*"],
        disallowed_tools=["Bash", "Write", "Edit"],
        permission_mode="dontAsk",
        max_turns=MAX_TURNS,
        max_budget_usd=MAX_BUDGET_USD,
        output_format={"type": "json_schema", "schema": QUICK_BITE_SCHEMA},
    )
    failed = []
    async for message in query(prompt=prompt, options=options):
        if isinstance(message, SystemMessage) and message.subtype == "init":
            failed = [s for s in message.data.get("mcp_servers", []) if s.get("status") != "connected"]
            if failed:
                print(f"MCP servers not connected: {failed}")
        if isinstance(message, ResultMessage):
            cost = message.total_cost_usd or 0.0
            if message.subtype != "success" or not message.structured_output:
                return Investigation(None, tools.charts, cost, message.num_turns,
                                     error=f"{message.subtype}: {message.errors or message.result}", failed_mcp_servers=failed)
            return Investigation(message.structured_output, tools.charts, cost, message.num_turns, failed_mcp_servers=failed)
    return Investigation(None, tools.charts, 0.0, 0, error="Agent ended without a result")


def investigate(db_connector, chain_name: str, origin_key: str, spikes: list[dict], metric_labels: dict) -> Investigation:
    tools = InvestigationTools(db_connector)
    prompt = task_prompt(chain_name, origin_key, spikes, metric_labels)
    try:
        return asyncio.run(_run(prompt, tools))
    except Exception as e:  # query() raises after yielding an error result
        return Investigation(None, tools.charts, 0.0, 0, error=str(e))
