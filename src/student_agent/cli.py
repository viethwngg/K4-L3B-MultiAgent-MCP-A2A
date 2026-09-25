from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import replace
from pathlib import Path

import anyio
import httpx2

from .cases import load_case_set
from .config import Settings
from .contracts import Contracts
from .mcp_gateway import connect_gateway
from .submission import package_submission, validate_artifacts
from .trace import TraceWriter
from .workflow import solve_case


def _root(value: str) -> Path:
    return Path(value).resolve()


async def _show_tools(root: Path) -> None:
    settings = Settings.load(root)
    contracts = Contracts(root / "contracts" / "schemas")
    async with connect_gateway(settings.mcp_endpoint, settings.team_api_key, contracts) as gateway:
        for tool in await gateway.list_tools():
            print(tool)


def _transient(exc: BaseException) -> bool:
    if isinstance(exc, BaseExceptionGroup):
        return all(_transient(child) for child in exc.exceptions)
    return isinstance(
        exc,
        (
            httpx2.TransportError,
            ConnectionError,
            TimeoutError,
            anyio.EndOfStream,
            anyio.BrokenResourceError,
        ),
    )


async def _run(root: Path, *, resume: bool = False, workers: int = 4) -> None:
    settings = Settings.load(root)
    case_set = load_case_set(root)
    contracts = Contracts(root / "contracts" / "schemas")
    output_root = root / "outputs"
    trace_path = root / "traces" / "trace.jsonl"
    output_root.mkdir(parents=True, exist_ok=True)
    trace_path.parent.mkdir(parents=True, exist_ok=True)
    completed: set[str] = set()
    if resume:
        for path in output_root.glob("*.json"):
            output = json.loads(path.read_text(encoding="utf-8"))
            contracts.validate_output(output, str(path))
            if path.stem not in case_set.cases or output["case_id"] != path.stem:
                raise ValueError(f"Cannot resume unexpected output: {path.name}")
            completed.add(path.stem)
        lines = trace_path.read_text(encoding="utf-8").splitlines() if trace_path.exists() else []
        retained = [line for line in lines if json.loads(line)["case_id"] in completed]
        if len(retained) != len(lines):
            backup = trace_path.with_name(f"interrupted-{time.time_ns()}.jsonl")
            backup.write_text("\n".join(lines) + "\n", encoding="utf-8")
            trace_path.write_text("\n".join(retained) + "\n", encoding="utf-8")
        if completed:
            partial = replace(case_set, case_ids=tuple(sorted(completed)))
            validate_artifacts(root, partial, contracts)
        print(f"Resuming: {len(completed)}/{len(case_set.case_ids)} verified cases", flush=True)
    else:
        for stale in output_root.glob("*.json"):
            stale.unlink()
        trace_path.unlink(missing_ok=True)
    semaphore = asyncio.Semaphore(workers)

    async def run_case(case_id: str) -> None:
        async with semaphore:
            case = case_set.cases[case_id]
            for attempt in range(2):
                trace = TraceWriter(trace_path, contracts, buffered=True)
                try:
                    async with connect_gateway(
                        settings.mcp_endpoint, settings.team_api_key, contracts
                    ) as gateway:
                        if not await gateway.list_tools():
                            raise RuntimeError("MCP Gateway returned no tools")
                        trace.emit(case_id=case_id, event_type="case_received", actor="coordinator")
                        output = await solve_case(case, gateway, trace)
                        contracts.validate_output(output, f"outputs/{case_id}.json")
                        trace.emit(
                            case_id=case_id, event_type="case_finalized", actor="coordinator"
                        )
                    # Publish only after the case and connection close successfully.
                    target = output_root / f"{case_id}.json"
                    temporary = target.with_suffix(".json.tmp")
                    temporary.write_text(
                        json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
                    )
                    trace.flush()
                    temporary.replace(target)
                    completed.add(case_id)
                    print(
                        f"[{len(completed)}/{len(case_set.case_ids)}] {case_id}: "
                        f"{output['assessment']['primary_issue']}",
                        flush=True,
                    )
                    return
                except Exception as exc:
                    if attempt == 0 and _transient(exc):
                        print(f"{case_id}: connection interrupted; reconnecting once", flush=True)
                        await asyncio.sleep(1)
                        continue
                    raise

    await asyncio.gather(*(run_case(cid) for cid in case_set.case_ids if cid not in completed))


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Day09 L3B student workflow")
    result.add_argument("--root", default=".", help="repository root (default: current directory)")
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser("validate-inputs", help="validate case-set.json and all 100 inputs")
    commands.add_parser("mcp-tools", help="authenticate and list discovered MCP tools")
    run = commands.add_parser("run", help="run the implemented workflow for all cases")
    run.add_argument(
        "--resume", action="store_true", help="continue the current run's verified cases"
    )
    run.add_argument(
        "--workers",
        type=int,
        choices=range(1, 5),
        default=4,
        help="concurrent cases, from 1 to 4 (default: 4)",
    )
    commands.add_parser("validate", help="validate outputs and observable trace")
    package = commands.add_parser("package", help="validate and build the submission ZIP")
    package.add_argument("--output", default="dist/submission.zip")
    return result


def main() -> None:
    args = parser().parse_args()
    root = _root(args.root)
    try:
        if args.command == "validate-inputs":
            case_set = load_case_set(root)
            print(
                f"OK: {case_set.variant_id} / {case_set.version} / {len(case_set.case_ids)} cases"
            )
        elif args.command == "mcp-tools":
            asyncio.run(_show_tools(root))
        elif args.command == "run":
            asyncio.run(_run(root, resume=args.resume, workers=args.workers))
        elif args.command == "validate":
            case_set = load_case_set(root)
            contracts = Contracts(root / "contracts" / "schemas")
            _, trace = validate_artifacts(root, case_set, contracts)
            print(f"OK: {len(case_set.case_ids)} outputs / {len(trace)} trace events")
        elif args.command == "package":
            destination = package_submission(root, root / args.output)
            print(f"OK: {destination}")
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
