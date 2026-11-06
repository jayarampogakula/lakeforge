#!/usr/bin/env python
"""
LakeForge setup script for backwards compatibility with setuptools.
Metadata is configured in pyproject.toml.
"""
from setuptools import setup, find_packages

setup(
    name="lakeforge",
    version="1.0.0",
    description="Enterprise Databricks Medallion ETL, Data Quality, and Governance Framework",
    packages=find_packages(include=["lakeforge*"]),
    entry_points={
        "console_scripts": [
            "lakeforge=lakeforge.cli:main",
        ],
    },
    python_requires=">=3.9",
)
