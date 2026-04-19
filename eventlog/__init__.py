"""
eventlog — Configuration-driven event log builder for Databricks.

Build, enrich, and validate process event logs from any source tables
in Unity Catalog using declarative YAML configs.
"""

from eventlog.builder import EventLogBuilder
from eventlog.enricher import EventLogEnricher
from eventlog.validator import EventLogValidator

__all__ = ["EventLogBuilder", "EventLogEnricher", "EventLogValidator"]
