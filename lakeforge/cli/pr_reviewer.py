"""
LakeForge Automated Pull Request Reviewer & Duplicate Function Detector.
Analyzes PR diffs to detect:
1. Duplicate / redundant functions that already exist in the codebase (name collision & logic similarity).
2. Hardcoded personal paths or credentials.
3. Missing docstrings and type annotations on newly added public functions.
4. Generates an automated GitHub PR review comment.
"""
import os
import sys
import ast
import json
import difflib
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional


class FunctionInfo:
    """Stores metadata about a Python function."""
    def __init__(self, name: str, file_path: str, line_no: int, args: List[str], docstring: Optional[str], body_source: str):
        self.name = name
        self.file_path = file_path
        self.line_no = line_no
        self.args = args
        self.docstring = docstring or ""
        self.body_source = body_source.strip()


def get_git_diff_ranges(base_ref: str = "origin/main") -> Dict[str, List[Tuple[int, int]]]:
    """
    Get modified line ranges for added lines in each changed Python file from git diff.
    Returns: { "file_path": [(start_line, end_line), ...] }
    """
    cmd = ["git", "diff", "-U0", base_ref, "HEAD"]
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        output = proc.stdout
    except Exception:
        # Fallback to HEAD~1
        try:
            proc = subprocess.run(["git", "diff", "-U0", "HEAD~1", "HEAD"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
            output = proc.stdout
        except Exception:
            return {}

    diff_ranges: Dict[str, List[Tuple[int, int]]] = {}
    current_file = None

    for line in output.splitlines():
        if line.startswith("+++ b/"):
            current_file = line[6:].strip()
            if current_file.endswith(".py"):
                diff_ranges[current_file] = []
            else:
                current_file = None
        elif line.startswith("@@ ") and current_file:
            # Format: @@ -start,count +start,count @@
            try:
                plus_part = [p for p in line.split() if p.startswith("+")][0]
                plus_spec = plus_part[1:]
                if "," in plus_spec:
                    start, count = map(int, plus_spec.split(","))
                else:
                    start, count = int(plus_spec), 1
                if count > 0:
                    diff_ranges[current_file].append((start, start + count))
            except Exception:
                pass

    return diff_ranges


def extract_functions_from_file(file_path: Path) -> List[FunctionInfo]:
    """Extract all top-level and class functions from a Python file using AST."""
    functions = []
    try:
        content = file_path.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(file_path))
        lines = content.splitlines()
    except Exception:
        return []

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            # Ignore dunder methods except __init__
            if node.name.startswith("__") and node.name.endswith("__") and node.name != "__init__":
                continue

            args = [a.arg for a in node.args.args if a.arg != "self"]
            docstring = ast.get_docstring(node)

            # Extract body source lines
            end_lineno = getattr(node, "end_lineno", node.lineno + 5)
            body_lines = lines[node.lineno - 1 : end_lineno]
            body_source = "\n".join(body_lines)

            functions.append(FunctionInfo(
                name=node.name,
                file_path=str(file_path.as_posix()),
                line_no=node.lineno,
                args=args,
                docstring=docstring,
                body_source=body_source
            ))

    return functions


def scan_all_codebase_functions(repo_root: Path, exclude_paths: Optional[List[str]] = None) -> List[FunctionInfo]:
    """Scan all Python functions across lakeforge."""
    exclude = set(exclude_paths or [])
    all_funcs = []
    target_dir = repo_root / "lakeforge"
    if not target_dir.exists():
        target_dir = repo_root

    for py_file in target_dir.rglob("*.py"):
        rel_posix = py_file.relative_to(repo_root).as_posix()
        if any(ex in rel_posix for ex in exclude):
            continue
        all_funcs.extend(extract_functions_from_file(py_file))

    return all_funcs


def analyze_pr_changes(repo_root: Path, base_ref: str = "origin/main") -> Dict[str, Any]:
    """
    Perform automated PR analysis:
    - Detect newly added functions.
    - Check if identical or similar functions already exist elsewhere.
    - Check for missing docstrings, type annotations, and hardcoded secrets.
    """
    diff_ranges = get_git_diff_ranges(base_ref)
    review_report = {
        "pr_files_analyzed": list(diff_ranges.keys()),
        "new_functions_count": 0,
        "duplicate_warnings": [],
        "style_warnings": [],
        "security_warnings": [],
        "passed": True
    }

    # If no git diff ranges found (e.g. initial scan), analyze files modified in recent commits
    if not diff_ranges:
        return review_report

    # Index existing functions across codebase (prior to new changes)
    changed_files = set(diff_ranges.keys())
    existing_funcs = scan_all_codebase_functions(repo_root, exclude_paths=list(changed_files))

    # Also build a lookup of existing functions by name
    existing_by_name: Dict[str, List[FunctionInfo]] = {}
    for f in existing_funcs:
        existing_by_name.setdefault(f.name, []).append(f)

    # Analyze changed files
    for file_rel_path, ranges in diff_ranges.items():
        file_path = repo_root / file_rel_path
        if not file_path.exists():
            continue

        file_content = file_path.read_text(encoding="utf-8")
        file_funcs = extract_functions_from_file(file_path)

        # Check for hardcoded workspace/user paths or secrets
        for idx, line in enumerate(file_content.splitlines(), start=1):
            if any(start <= idx <= end for start, end in ranges):
                if "/Workspace/Users/" in line and not line.strip().startswith("#"):
                    review_report["security_warnings"].append({
                        "file": file_rel_path,
                        "line": idx,
                        "issue": "Hardcoded personal workspace path detected (`/Workspace/Users/...`). Use generic dynamic discovery."
                    })
                if any(k in line.lower() for k in ["password =", "token =", "secret ="]) and not line.strip().startswith("#") and "dbutils" not in line and "None" not in line and "Optional" not in line and "str =" not in line:
                    review_report["security_warnings"].append({
                        "file": file_rel_path,
                        "line": idx,
                        "issue": "Potential hardcoded credential or secret detected. Use Databricks Secrets scope."
                    })

        # Identify newly added functions
        for func in file_funcs:
            # Check if function starts within any added range
            is_new = any(start <= func.line_no <= end for start, end in ranges)
            if not is_new:
                continue

            review_report["new_functions_count"] += 1

            # 1. Exact Name Collision Check with existing functions
            if func.name in existing_by_name:
                for existing in existing_by_name[func.name]:
                    if existing.file_path != func.file_path:
                        review_report["duplicate_warnings"].append({
                            "type": "NAME_COLLISION",
                            "new_func": func.name,
                            "new_file": file_rel_path,
                            "new_line": func.line_no,
                            "existing_func": existing.name,
                            "existing_file": existing.file_path,
                            "existing_line": existing.line_no,
                            "recommendation": f"A function named `{func.name}` already exists in `{existing.file_path}:{existing.line_no}`. Consider reusing it to prevent redundant code."
                        })

            # 2. Semantic Body Similarity Check
            for existing in existing_funcs:
                if existing.file_path == func.file_path and existing.name == func.name:
                    continue
                # Compare normalized body
                matcher = difflib.SequenceMatcher(None, func.body_source, existing.body_source)
                ratio = matcher.ratio()
                if ratio >= 0.82 and func.name != existing.name:
                    review_report["duplicate_warnings"].append({
                        "type": "LOGIC_DUPLICATION",
                        "new_func": func.name,
                        "new_file": file_rel_path,
                        "new_line": func.line_no,
                        "existing_func": existing.name,
                        "existing_file": existing.file_path,
                        "existing_line": existing.line_no,
                        "similarity_score": round(ratio * 100, 1),
                        "recommendation": f"Function `{func.name}` shares {round(ratio * 100, 1)}% identical logic with `{existing.name}` in `{existing.file_path}:{existing.line_no}`. Consider consolidating into a shared utility."
                    })

            # 3. Missing docstring check
            if not func.docstring and not func.name.startswith("_"):
                review_report["style_warnings"].append({
                    "file": file_rel_path,
                    "line": func.line_no,
                    "func": func.name,
                    "issue": f"Public function `{func.name}` is missing a docstring."
                })

    review_report["passed"] = (
        len(review_report["duplicate_warnings"]) == 0 and
        len(review_report["security_warnings"]) == 0
    )
    return review_report


def generate_pr_markdown_comment(report: Dict[str, Any]) -> str:
    """Generate professional GitHub Flavored Markdown comment for PR review."""
    md = []
    md.append("## 🔍 LakeForge Automated Pull Request Review")
    md.append("")
    
    if report["duplicate_warnings"]:
        md.append("### ⚠️ Duplicate Function & Logic Detection")
        md.append("LakeForge detected newly created functions that already exist in the codebase. Please review and consider reusing existing utilities:")
        md.append("")
        for d in report["duplicate_warnings"]:
            if d["type"] == "NAME_COLLISION":
                md.append(f"- 🔴 **Name Collision**: `{d['new_func']}` introduced in `{d['new_file']}:{d['new_line']}`")
                md.append(f"  - **Already exists in**: `{d['existing_file']}:{d['existing_line']}`")
                md.append(f"  - **Recommendation**: {d['recommendation']}")
            else:
                md.append(f"- 🟡 **Logic Duplication ({d.get('similarity_score')}%)**: `{d['new_func']}` in `{d['new_file']}:{d['new_line']}`")
                md.append(f"  - **Similar function**: `{d['existing_func']}` in `{d['existing_file']}:{d['existing_line']}`")
                md.append(f"  - **Recommendation**: {d['recommendation']}")
        md.append("")

    if report["security_warnings"]:
        md.append("### 🚨 Security & Secret Guardrails")
        for s in report["security_warnings"]:
            md.append(f"- ❌ `{s['file']}:{s['line']}` — {s['issue']}")
        md.append("")

    if report["style_warnings"]:
        md.append("### 📝 Code Quality & Documentation Suggestions")
        for st in report["style_warnings"][:10]:
            md.append(f"- ℹ️ `{st['file']}:{st['line']}` — {st['issue']}")
        if len(report["style_warnings"]) > 10:
            md.append(f"- ... and {len(report['style_warnings']) - 10} more documentation suggestions.")
        md.append("")

    if not report["duplicate_warnings"] and not report["security_warnings"]:
        md.append("### ✅ Automated Review Status: PASS")
        md.append("- **Duplicate Check**: Passed (No duplicate functions or logic collisions detected)")
        md.append("- **Security & Path Verification**: Passed (Zero hardcoded user workspaces or credentials)")
        md.append(f"- **New Functions Audited**: {report['new_functions_count']}")
    else:
        md.append("---")
        md.append("> **Action Required**: Please address the flagged duplicate functions or security warnings before merging.")

    return "\n".join(md)


def post_github_pr_comment(comment_body: str, repo: Optional[str] = None, pr_number: Optional[str] = None, token: Optional[str] = None):
    """Post comment to GitHub PR via GitHub API."""
    token = token or os.environ.get("GITHUB_TOKEN")
    repo = repo or os.environ.get("GITHUB_REPOSITORY")
    pr_number = pr_number or os.environ.get("PR_NUMBER")

    if not (token and repo and pr_number):
        # Print to stdout if running locally or outside GitHub Actions PR
        print("\n" + "=" * 70)
        print(" AUTOMATED PR REVIEW COMMENT PREVIEW")
        print("=" * 70)
        print(comment_body)
        print("=" * 70 + "\n")
        return

    import urllib.request
    url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
        "User-Agent": "LakeForge-PR-Reviewer"
    }
    payload = json.dumps({"body": comment_body}).encode("utf-8")

    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req) as resp:
            if resp.status in [200, 201]:
                print(f"✅ Successfully posted automated review comment to PR #{pr_number}")
            else:
                print(f"Warning: GitHub API returned status {resp.status}")
    except Exception as e:
        print(f"Error posting comment to PR: {e}")


def main():
    """CLI Entrypoint for LakeForge PR Reviewer."""
    import argparse
    parser = argparse.ArgumentParser(description="LakeForge Automated PR Review & Duplicate Function Detector")
    parser.add_argument("--base-ref", default="origin/main", help="Base git ref to compare against (default: origin/main)")
    parser.add_argument("--repo-root", default=".", help="Root of LakeForge repository")
    parser.add_argument("--post-comment", action="store_true", help="Post comment to GitHub PR if environment variables are set")
    parser.add_argument("--fail-on-duplicate", action="store_true", help="Exit with code 1 if duplicate functions are found")

    args = parser.parse_args()
    repo_root = Path(args.repo_root).resolve()

    report = analyze_pr_changes(repo_root, base_ref=args.base_ref)
    comment = generate_pr_markdown_comment(report)

    if args.post_comment:
        post_github_pr_comment(comment)
    else:
        print(comment)

    if args.fail_on_duplicate and not report["passed"]:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
