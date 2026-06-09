"""
LakeForge Reporting Module
Generates JSON and HTML trust score reports.
"""
from typing import Dict, List, Any, Optional
from datetime import datetime
import json


class ReportGenerator:
    """Generate trust score reports in JSON and HTML formats."""
    
    def __init__(self):
        pass
    
    def generate_trust_report(
        self,
        dq_results: Optional[Dict[str, Any]] = None,
        trust_results: Optional[Dict[str, Any]] = None,
        schema_drift_results: Optional[Dict[str, Any]] = None,
        pipeline_name: str = "Pipeline",
        report_title: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate combined trust report."""
        report_title = report_title or f"{pipeline_name} Trust Report"
        
        scores = []
        if dq_results:
            dq_score = (dq_results.get("rules_passed", 0) / dq_results.get("rules_executed", 1)) * 100
            scores.append(dq_score)
        if trust_results:
            scores.append(trust_results.get("trust_score", 0))
        if schema_drift_results:
            drift_score = schema_drift_results.get("drift_score", 1.0) * 100
            scores.append(drift_score)
        
        overall_trust_score = sum(scores) / len(scores) if scores else 0
        
        if overall_trust_score >= 95:
            trust_level, trust_color = "EXCELLENT", "#28a745"
        elif overall_trust_score >= 85:
            trust_level, trust_color = "GOOD", "#5cb85c"
        elif overall_trust_score >= 70:
            trust_level, trust_color = "ACCEPTABLE", "#f0ad4e"
        elif overall_trust_score >= 50:
            trust_level, trust_color = "POOR", "#d9534f"
        else:
            trust_level, trust_color = "CRITICAL", "#c9302c"
        
        return {
            "report_title": report_title,
            "pipeline_name": pipeline_name,
            "timestamp": datetime.now().isoformat(),
            "overall_trust_score": round(overall_trust_score, 2),
            "trust_level": trust_level,
            "trust_color": trust_color,
            "data_quality": dq_results,
            "trust_validations": trust_results,
            "schema_drift": schema_drift_results,
            "summary": {
                "total_checks": (
                    (dq_results.get("rules_executed", 0) if dq_results else 0) +
                    (trust_results.get("total_validations", 0) if trust_results else 0) +
                    (1 if schema_drift_results else 0)
                ),
                "passed_checks": (
                    (dq_results.get("rules_passed", 0) if dq_results else 0) +
                    (trust_results.get("passed_count", 0) if trust_results else 0) +
                    (1 if schema_drift_results and not schema_drift_results.get("has_drift") else 0)
                ),
                "failed_checks": (
                    (dq_results.get("rules_failed", 0) if dq_results else 0) +
                    (trust_results.get("failed_count", 0) if trust_results else 0) +
                    (1 if schema_drift_results and schema_drift_results.get("has_drift") else 0)
                )
            }
        }
    
    def save_json_report(self, report: Dict[str, Any], output_path: str):
        """Save report as JSON file."""
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
    
    def generate_html_report(self, report: Dict[str, Any]) -> str:
        """Generate HTML report from trust report."""
        
        css = """
        <style>
            body { font-family: Arial, sans-serif; background: #f5f5f5; padding: 20px; }
            .container { max-width: 1200px; margin: 0 auto; background: white; border-radius: 8px; }
            .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                     color: white; padding: 30px; }
            .trust-score { text-align: center; padding: 40px; background: #f8f9fa; }
            .trust-score .score { font-size: 72px; font-weight: bold; }
            .summary { display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; padding: 30px; }
            .summary-card { background: white; padding: 20px; border-radius: 6px; 
                           border: 1px solid #e0e0e0; text-align: center; }
            .section { padding: 30px; border-top: 1px solid #e0e0e0; }
            .validation-item { background: #f8f9fa; padding: 15px; margin-bottom: 10px; 
                              border-radius: 4px; border-left: 4px solid #ddd; }
            .validation-item.passed { border-left-color: #28a745; }
            .validation-item.failed { border-left-color: #dc3545; }
            .badge { display: inline-block; padding: 4px 12px; border-radius: 12px; 
                    font-size: 12px; font-weight: 600; }
            .badge.passed { background: #d4edda; color: #155724; }
            .badge.failed { background: #f8d7da; color: #721c24; }
        </style>
        """
        
        html = f"""<!DOCTYPE html>
<html>
<head><title>{report['report_title']}</title>{css}</head>
<body>
<div class="container">
    <div class="header">
        <h1>{report['report_title']}</h1>
        <div>Generated: {report['timestamp']}</div>
    </div>
    <div class="trust-score">
        <div class="score" style="color: {report['trust_color']}">{report['overall_trust_score']}%</div>
        <div style="font-size: 24px; color: {report['trust_color']}">{report['trust_level']}</div>
    </div>
    <div class="summary">
        <div class="summary-card"><div style="font-size: 36px">{report['summary']['total_checks']}</div><div>Total Checks</div></div>
        <div class="summary-card"><div style="font-size: 36px; color: #28a745">{report['summary']['passed_checks']}</div><div>Passed</div></div>
        <div class="summary-card"><div style="font-size: 36px; color: #dc3545">{report['summary']['failed_checks']}</div><div>Failed</div></div>
    </div>
"""
        
        if report.get("data_quality"):
            dq = report["data_quality"]
            html += f'<div class="section"><h2>Data Quality</h2><p>Score: {dq.get("rules_passed", 0)}/{dq.get("rules_executed", 0)}</p>'
            for rule in dq.get("rule_results", []):
                status = "passed" if rule.get("passed") else "failed"
                html += f'<div class="validation-item {status}"><strong>{rule.get("rule_name")}</strong> '
                html += f'<span class="badge {status}">{"PASSED" if rule.get("passed") else "FAILED"}</span></div>'
            html += '</div>'
        
        if report.get("trust_validations"):
            trust = report["trust_validations"]
            html += f'<div class="section"><h2>Trust Validations</h2><p>Score: {trust.get("trust_score")}%</p>'
            for val in trust.get("validation_results", []):
                status = "passed" if val.get("passed") else "failed"
                html += f'<div class="validation-item {status}"><strong>{val.get("validation")}</strong> '
                html += f'<span class="badge {status}">{"PASSED" if val.get("passed") else "FAILED"}</span><div>{val.get("message")}</div></div>'
            html += '</div>'
        
        html += '</div></body></html>'
        return html
    
    def save_html_report(self, report: Dict[str, Any], output_path: str):
        """Save report as HTML file."""
        html = self.generate_html_report(report)
        with open(output_path, 'w') as f:
            f.write(html)


def create_report_generator() -> ReportGenerator:
    return ReportGenerator()
