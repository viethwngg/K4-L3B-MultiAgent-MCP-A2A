import subprocess
from pathlib import Path


def test_repository_contains_no_competition_payload() -> None:
    root = Path(__file__).resolve().parents[1]
    # Runtime inputs and outputs are expected locally; only committed release files
    # must be free of competition payloads.
    tracked = (
        subprocess.check_output(["git", "ls-files", "-z"], cwd=root).decode("utf-8").split("\0")
    )
    assert "case-set.json" not in tracked
    assert not any(
        name.startswith(("inputs/", "outputs/")) and name.endswith(".json") for name in tracked
    )
    forbidden = {"oracles", "reference-outputs", "private-partitions.json", "mcp-access.json"}
    assert not any(part in forbidden for name in tracked for part in Path(name).parts)


def test_example_environment_has_no_real_key() -> None:
    root = Path(__file__).resolve().parents[1]
    content = (root / ".env.example").read_text(encoding="utf-8")
    assert "sk-team-replace_me" in content
    assert content.count("sk-team-") == 1
