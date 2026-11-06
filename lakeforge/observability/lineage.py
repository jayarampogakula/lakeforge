"""
LakeForge Observability - Pipeline Lineage Tracker.
Captures input/output dataset relationships across Bronze, Silver, Gold, and Views.
"""
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import json
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class LineageNode:
    """Represents a dataset in the Lakehouse lineage graph."""
    dataset_name: str
    layer: str  # source, bronze, silver, gold, view
    catalog: Optional[str] = None
    schema: Optional[str] = None
    client_id: Optional[str] = None

    @property
    def fully_qualified_name(self) -> str:
        parts = [p for p in [self.catalog, self.schema, self.dataset_name] if p]
        return ".".join(parts)


@dataclass
class LineageEdge:
    """Represents data movement / transformation from inputs to output."""
    pipeline_name: str
    inputs: List[LineageNode]
    output: LineageNode
    operation: str  # ingest, clean, aggregate, view_projection
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "operation": self.operation,
            "timestamp": self.timestamp,
            "inputs": [asdict(n) for n in self.inputs],
            "output": asdict(self.output)
        }


class LineageTracker:
    """Records and emits pipeline lineage events."""

    def __init__(self):
        self.edges: List[LineageEdge] = []
        self.logger = logging.getLogger(__name__)

    def record_edge(
        self,
        pipeline_name: str,
        operation: str,
        inputs: List[LineageNode],
        output: LineageNode
    ) -> LineageEdge:
        edge = LineageEdge(
            pipeline_name=pipeline_name,
            operation=operation,
            inputs=inputs,
            output=output
        )
        self.edges.append(edge)
        self.logger.info(
            f"Lineage recorded: {[i.fully_qualified_name for i in inputs]} -> "
            f"[{operation}] -> {output.fully_qualified_name}"
        )
        return edge

    def export_graph(self) -> List[Dict[str, Any]]:
        return [e.to_dict() for e in self.edges]
