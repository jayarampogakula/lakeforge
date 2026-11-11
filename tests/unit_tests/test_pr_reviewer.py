"""
Unit tests for LakeForge Automated PR Reviewer & Duplicate Function Detector.
"""
import pytest
from pathlib import Path
from lakeforge.cli.pr_reviewer import (
    extract_functions_from_file,
    scan_all_codebase_functions,
    generate_pr_markdown_comment
)


def test_extract_functions_from_file(tmp_path):
    """Verify AST extraction of function metadata, arguments, and docstrings."""
    py_code = """
def sample_function(a: int, b: str = "default"):
    \"\"\"Sample docstring.\"\"\"
    return f"{a}_{b}"

class Helper:
    def helper_method(self, x):
        return x * 2
"""
    test_file = tmp_path / "test_sample.py"
    test_file.write_text(py_code, encoding="utf-8")

    funcs = extract_functions_from_file(test_file)
    func_names = [f.name for f in funcs]
    assert "sample_function" in func_names
    assert "helper_method" in func_names

    sample = next(f for f in funcs if f.name == "sample_function")
    assert sample.args == ["a", "b"]
    assert sample.docstring == "Sample docstring."


def test_generate_pr_markdown_comment():
    """Verify GitHub PR markdown review comment formatting."""
    clean_report = {
        "pr_files_analyzed": ["lakeforge/sample.py"],
        "new_functions_count": 2,
        "duplicate_warnings": [],
        "style_warnings": [],
        "security_warnings": [],
        "passed": True
    }
    clean_md = generate_pr_markdown_comment(clean_report)
    assert "### ✅ Automated Review Status: PASS" in clean_md
    assert "Duplicate Check" in clean_md

    # Test with duplicate warning
    dup_report = {
        "pr_files_analyzed": ["lakeforge/utils/date.py"],
        "new_functions_count": 1,
        "duplicate_warnings": [
            {
                "type": "NAME_COLLISION",
                "new_func": "add_audit_columns",
                "new_file": "lakeforge/utils/date.py",
                "new_line": 15,
                "existing_func": "add_audit_columns",
                "existing_file": "lakeforge/bronze/bronze_writer.py",
                "existing_line": 34,
                "recommendation": "A function named `add_audit_columns` already exists in `lakeforge/bronze/bronze_writer.py:34`."
            }
        ],
        "style_warnings": [],
        "security_warnings": [],
        "passed": False
    }
    dup_md = generate_pr_markdown_comment(dup_report)
    assert "### ⚠️ Duplicate Function & Logic Detection" in dup_md
    assert "add_audit_columns" in dup_md
    assert "Action Required" in dup_md
