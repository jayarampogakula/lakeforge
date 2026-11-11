"""
LakeForge CLI - Central Stage Registry.

`configs/environments/env_settings.json` is the SINGLE source of truth for
deployment stage names: its top-level keys are the stages, its per-stage values
are the environment settings (catalogs, schemas, patterns).

This module:
  1. Loads the stage registry (load_stage_registry) - used by `lakeforge deploy`
     for its supported-stage list and by CI workflows for runtime validation.
  2. Regenerates the `targets:` block of every Databricks bundle from the
     registry: the root bundle, deployments/_template/, and each
     deployments/<client_id>/ bundle (sync_stages / `lakeforge sync-stages`).

Conventions (preserved when regenerating):
  - The stage named "dev" (if present in the registry) is the development
    sandbox target: default target, no `mode:` (keeps clean job names),
    user-scoped root_path `~/.lakeforge[_<client>]/dev`.
  - Every other stage is a production target: `mode: production`,
    shared root_path `/Workspace/Shared/lakeforge[_<client>]/<stage>`.

To add/rename/remove a stage: edit env_settings.json, then run
`lakeforge sync-stages` and commit. CI stage inputs are validated against the
registry at runtime, so no workflow YAML needs editing.
"""
import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional, Tuple

REGISTRY_RELPATH = "environments/env_settings.json"
DEPLOYMENTS_DIR = "deployments"
TEMPLATE_DIR = "_template"

_DEV_COMMENT = (
    "    # No 'mode: development' on purpose: it would prefix all job names with",
    '    # "[dev <username>]". Plain target keeps clean names; root_path stays',
    "    # user-scoped below to keep personal deployments isolated.",
)
_PROD_COMMENT = (
    "    # Production mode: locked bundle state, no uncommitted changes permitted",
)


def _resolve_registry_path(config_dir: Optional[str] = None) -> Path:
    """Locate configs/environments/env_settings.json from an explicit config dir or by walking up from CWD."""
    candidates: List[Path] = []
    if config_dir:
        candidates.append(Path(config_dir) / REGISTRY_RELPATH)
    here = Path.cwd()
    for base in [here, *here.parents]:
        candidates.append(base / "configs" / REGISTRY_RELPATH)
        candidates.append(base / REGISTRY_RELPATH)
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        "Stage registry not found: expected configs/environments/env_settings.json. "
        "Run from the LakeForge repository root (or pass --config-dir)."
    )


def find_repo_root(start: Optional[Path] = None) -> Optional[Path]:
    """Walk upward to the directory containing configs/environments/env_settings.json."""
    p = (start or Path.cwd()).resolve()
    for candidate in [p, *p.parents]:
        if (candidate / "configs" / REGISTRY_RELPATH).is_file():
            return candidate
    return None


def load_stage_registry(config_dir: Optional[str] = None) -> List[str]:
    """
    Return the ordered list of stage names defined by the registry
    (the top-level keys of configs/environments/env_settings.json).
    """
    path = _resolve_registry_path(config_dir)
    with open(path, encoding="utf-8") as fh:
        stages = list(json.load(fh).keys())
    if not stages:
        raise ValueError(f"Stage registry {path} defines no stages.")
    return stages


_DEFAULT_FALLBACK_STAGES = ["dev", "test", "staging", "prod"]


def supported_stages(config_dir: Optional[str] = None) -> List[str]:
    """
    Stages from the registry. Falls back to the classic set ONLY when the
    registry cannot be located (e.g. the module is imported outside the
    repository), so tooling keeps a sane default while the repo stays
    registry-driven.
    """
    try:
        return load_stage_registry(config_dir)
    except (FileNotFoundError, ValueError):
        return list(_DEFAULT_FALLBACK_STAGES)


def _render_sandbox_target(stage: str, dev_prefix: str) -> List[str]:
    lines = [f"  {stage}:"]
    lines.extend(_DEV_COMMENT)
    lines += [
        "    default: true",
        "    workspace:",
        f"      root_path: ~/{dev_prefix}/dev",
        "    variables:",
        f'      stage: "{stage}"',
    ]
    return lines


def _render_production_target(stage: str, prefix: str) -> List[str]:
    lines = [f"  {stage}:"]
    lines.extend(_PROD_COMMENT)
    lines += [
        "    mode: production",
        "    workspace:",
        f"      root_path: /Workspace/Shared/{prefix}/{stage}",
        "    variables:",
        f'      stage: "{stage}"',
    ]
    return lines


def render_targets(stages: List[str], bundle_scope: str, client: Optional[str] = None) -> str:
    """
    Render the full `targets:` block for a bundle databricks.yml.

    bundle_scope "global": root bundle - prefix is always `lakeforge`.
    bundle_scope "client": per-client bundle - prefix is `lakeforge_<client>`;
      `client` may be a literal client id or the __CLIENT_ID__ template placeholder.
    """
    if bundle_scope == "client" and client:
        dev_prefix = f".lakeforge_{client}"
        shared_prefix = f"lakeforge_{client}"
    else:
        dev_prefix = ".lakeforge"
        shared_prefix = "lakeforge"

    blocks: List[List[str]] = []
    for stage in stages:
        if stage == "dev":
            blocks.append(_render_sandbox_target(stage, dev_prefix))
        else:
            blocks.append(_render_production_target(stage, shared_prefix))

    lines = ["targets:"]
    for i, block in enumerate(blocks):
        if i:
            lines.append("")
        lines.extend(block)
    return "\n".join(lines) + "\n"


def _expected_targets(yml_path: Path, stages: List[str], bundle_scope: str, client: Optional[str]) -> Tuple[str, str]:
    """Return (current_text, expected_text) for a bundle file."""
    current = yml_path.read_text(encoding="utf-8")
    marker = "\ntargets:"
    idx = current.find(marker)
    if idx == -1:
        raise ValueError(f"No 'targets:' block found in {yml_path} - regenerate skipped.")
    expected = current[: idx + 1] + render_targets(stages, bundle_scope, client)
    return current, expected


def discover_bundles(repo_root: Path) -> List[Tuple[Path, str, Optional[str]]]:
    """All bundles to keep in sync: (databricks.yml path, scope, client id or placeholder)."""
    bundles: List[Tuple[Path, str, Optional[str]]] = [(repo_root / "databricks.yml", "global", None)]
    deployments = repo_root / DEPLOYMENTS_DIR
    if deployments.is_dir():
        for child in sorted(deployments.iterdir()):
            yml = child / "databricks.yml"
            if not yml.is_file():
                continue
            if child.name == TEMPLATE_DIR:
                bundles.append((yml, "client", "__CLIENT_ID__"))
            elif child.is_dir():
                bundles.append((yml, "client", child.name))
    return bundles


def sync_stages(check_only: bool = False, config_dir: Optional[str] = None) -> int:
    """Regenerate every bundle's targets: block from the stage registry. Returns count of files changed."""
    repo_root = find_repo_root()
    if repo_root is None:
        print("ERROR: run from the LakeForge repository root (configs/environments/env_settings.json not found).", file=sys.stderr)
        return 1

    stages = load_stage_registry(config_dir)
    print(f"Stage registry: {', '.join(stages)}")

    changed = 0
    for yml_path, scope, client in discover_bundles(repo_root):
        rel = yml_path.relative_to(repo_root).as_posix()
        try:
            current, expected = _expected_targets(yml_path, stages, scope, client)
        except ValueError as e:
            print(f"  SKIP  {rel}: {e}")
            continue
        if current == expected:
            print(f"  OK    {rel} (already in sync)")
        else:
            changed += 1
            if check_only:
                print(f"  DRIFT {rel} (targets do not match the registry - run without --check to apply)")
            else:
                yml_path.write_text(expected, encoding="utf-8")
                print(f"  WROTE {rel} (targets regenerated from registry)")

    if check_only:
        print(f"\nDry run complete: {changed} file(s) out of sync with the stage registry.")
    else:
        print(f"\nSync complete: {changed} bundle file(s) updated.")
    return 0


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser(
        prog="lakeforge sync-stages",
        description=(
            "Regenerate the targets: block of every Databricks bundle from the stage "
            "registry (configs/environments/env_settings.json keys)."
        ),
    )
    parser.add_argument("--check", action="store_true", help="Dry run: only report files out of sync, change nothing")
    parser.add_argument("--config-dir", default="configs", help="Path to config directory (default: configs)")
    args = parser.parse_args(argv)
    sys.exit(sync_stages(check_only=args.check, config_dir=args.config_dir))


if __name__ == "__main__":
    main()
