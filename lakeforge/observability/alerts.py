"""
LakeForge Observability - Alert Dispatcher.
Dispatches alert notifications to Webhooks, Slack, MS Teams, and Console on pipeline failures or DQ violations.
"""
import json
import logging
from typing import Dict, Any, Optional

try:
    import urllib.request
    HAS_URLLIB = True
except ImportError:
    HAS_URLLIB = False

logger = logging.getLogger(__name__)


class AlertDispatcher:
    """Dispatches operational alerts to configured endpoints."""

    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url
        self.logger = logging.getLogger(__name__)

    def send_alert(
        self,
        title: str,
        message: str,
        severity: str = "ERROR",  # INFO, WARNING, ERROR, CRITICAL
        context: Optional[Dict[str, Any]] = None,
        webhook_url: Optional[str] = None
    ) -> bool:
        """
        Send an alert message.
        """
        url = webhook_url or self.webhook_url
        payload = {
            "title": f"[{severity.upper()}] LakeForge Alert: {title}",
            "text": message,
            "severity": severity,
            "context": context or {}
        }

        # Log always
        log_level = logging.ERROR if severity in ["ERROR", "CRITICAL"] else logging.WARNING
        self.logger.log(log_level, f"ALERT [{severity}]: {title} - {message} (Context: {context})")

        if not url:
            return True

        if HAS_URLLIB:
            try:
                data = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return resp.status in [200, 201, 204]
            except Exception as e:
                self.logger.warning(f"Failed to post alert webhook to {url}: {str(e)}")
                return False

        return False


def create_alert_dispatcher(webhook_url: Optional[str] = None) -> AlertDispatcher:
    """Factory function for AlertDispatcher."""
    return AlertDispatcher(webhook_url=webhook_url)
