"""
Structured parse-time logger for XBRL extraction.

Captures errors, warnings, and informational observations that occur during
fact/concept/relationship extraction so they appear in the parsed output dict
rather than being silently swallowed.
"""

import time
from enum import Enum
from typing import Any, Dict, List, Optional, Union


class Severity(str, Enum):
    """Severity level for parse log entries."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class ParseLogger:
    """Collects parse-time entries into a JSON-serializable structure.

    Instantiate once per parse call; pass to extraction methods so they
    can record field-level failures instead of silently dropping data.

    Usage::

        log = ParseLogger()
        # ... extraction work ...
        log.log_error(
            section="facts",
            message=exc,
            concept="us-gaap:Revenue",
            context_ref="c-123",
            field="label",
        )
        log.log_warning(
            section="concepts",
            message="Label fallback used",
            concept="us-gaap:Assets",
            field="terse_label",
        )
        result["parse_log"] = log.to_dict()
    """

    def __init__(self) -> None:
        self._entries: List[Dict[str, Any]] = []
        self._section_counts: Dict[str, int] = {}
        self._start = time.monotonic()

    def _log(
        self,
        severity: Severity,
        section: str,
        message: Union[Exception, str],
        *,
        concept: Optional[str] = None,
        context_ref: Optional[str] = None,
        field: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> None:
        """Record a single parse log entry.

        Args:
            severity: Severity level for this entry.
            section: Parser section where the issue occurred
                     (e.g. "facts", "concepts", "presentation_relationships").
            message: The caught exception, or a plain string observation.
            concept: XBRL concept qname, if available.
            context_ref: Context ID, if available.
            field: The specific field that failed (e.g. "label", "data_type").
            detail: Optional free-text detail beyond the exception message.
        """
        if isinstance(message, str):
            source_type = "Observation"
            message_text = message
        else:
            source_type = type(message).__name__
            message_text = str(message)

        entry: Dict[str, Any] = {
            "severity": severity.value,
            "section": section,
            "source_type": source_type,
            "message": message_text,
        }
        if concept is not None:
            entry["concept"] = concept
        if context_ref is not None:
            entry["context_ref"] = context_ref
        if field is not None:
            entry["field"] = field
        if detail is not None:
            entry["detail"] = detail
        self._entries.append(entry)

    def log_error(
        self,
        section: str,
        message: Union[Exception, str],
        *,
        concept: Optional[str] = None,
        context_ref: Optional[str] = None,
        field: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> None:
        """Record an error-level entry (data integrity compromised)."""
        self._log(
            Severity.ERROR, section, message,
            concept=concept, context_ref=context_ref, field=field, detail=detail,
        )

    def log_warning(
        self,
        section: str,
        message: Union[Exception, str],
        *,
        concept: Optional[str] = None,
        context_ref: Optional[str] = None,
        field: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> None:
        """Record a warning-level entry (usable but degraded)."""
        self._log(
            Severity.WARNING, section, message,
            concept=concept, context_ref=context_ref, field=field, detail=detail,
        )

    def log_info(
        self,
        section: str,
        message: Union[Exception, str],
        *,
        concept: Optional[str] = None,
        context_ref: Optional[str] = None,
        field: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> None:
        """Record an info-level entry (enrichment gap, cosmetic)."""
        self._log(
            Severity.INFO, section, message,
            concept=concept, context_ref=context_ref, field=field, detail=detail,
        )

    def record_section_count(self, section: str, count: int) -> None:
        """Record how many items were extracted for a section."""
        self._section_counts[section] = count

    def log_aggregate(
        self,
        severity: Severity,
        section: str,
        message: str,
        *,
        count: int,
        field: Optional[str] = None,
    ) -> None:
        """Record a single summary entry representing *count* identical observations."""
        entry: Dict[str, Any] = {
            "severity": severity.value,
            "section": section,
            "source_type": "Aggregate",
            "message": message,
            "count": count,
        }
        if field is not None:
            entry["field"] = field
        self._entries.append(entry)

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable summary of all recorded entries."""
        error_count = sum(1 for e in self._entries if e["severity"] == Severity.ERROR.value)
        warning_count = sum(1 for e in self._entries if e["severity"] == Severity.WARNING.value)
        info_count = sum(1 for e in self._entries if e["severity"] == Severity.INFO.value)
        return {
            "error_count": error_count,
            "warning_count": warning_count,
            "info_count": info_count,
            "section_counts": dict(self._section_counts),
            "elapsed_seconds": round(time.monotonic() - self._start, 3),
            "entries": list(self._entries),
        }


class _NullParseLogger:
    """No-op stand-in when logging is not needed."""
    def log_error(self, *a, **kw): pass
    def log_warning(self, *a, **kw): pass
    def log_info(self, *a, **kw): pass
    def log_aggregate(self, *a, **kw): pass
    def record_section_count(self, *a, **kw): pass
    def to_dict(self):
        return {"error_count": 0, "warning_count": 0, "info_count": 0,
                "section_counts": {}, "elapsed_seconds": 0.0, "entries": []}


NULL_PARSE_LOGGER = _NullParseLogger()
