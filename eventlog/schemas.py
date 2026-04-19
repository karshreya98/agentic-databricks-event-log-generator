"""
YAML config schema for event log definitions.

Validates that user-provided configs have the required structure
before the builder attempts to generate a Spark pipeline.
"""


REQUIRED_TOP_LEVEL = {"event_log"}
REQUIRED_EVENT_LOG = {"name", "output_table", "sources"}
REQUIRED_SOURCE = {"table", "case_id", "events"}
REQUIRED_EVENT = {"activity", "timestamp"}
OPTIONAL_EVENT = {"condition", "resource", "cost"}
OPTIONAL_SOURCE = {"timestamp_format", "timezone"}
OPTIONAL_ENRICHMENT = {"table", "join_key", "columns"}


class ConfigValidationError(Exception):
    """Raised when a YAML config is invalid."""
    pass


def validate_config(config: dict) -> dict:
    """
    Validate an event log YAML config and return the event_log section.

    Raises ConfigValidationError with a descriptive message on failure.
    """
    if not isinstance(config, dict):
        raise ConfigValidationError("Config must be a dictionary")

    if "event_log" not in config:
        raise ConfigValidationError(
            f"Config must have top-level 'event_log' key. Found: {list(config.keys())}"
        )

    el = config["event_log"]

    missing = REQUIRED_EVENT_LOG - set(el.keys())
    if missing:
        raise ConfigValidationError(f"event_log missing required keys: {missing}")

    if not isinstance(el["sources"], list) or len(el["sources"]) == 0:
        raise ConfigValidationError("event_log.sources must be a non-empty list")

    for i, source in enumerate(el["sources"]):
        missing_src = REQUIRED_SOURCE - set(source.keys())
        if missing_src:
            raise ConfigValidationError(
                f"source[{i}] (table: {source.get('table', '?')}) missing: {missing_src}"
            )

        if not isinstance(source["events"], list) or len(source["events"]) == 0:
            raise ConfigValidationError(
                f"source[{i}] (table: {source['table']}) must have at least one event"
            )

        for j, event in enumerate(source["events"]):
            missing_evt = REQUIRED_EVENT - set(event.keys())
            if missing_evt:
                raise ConfigValidationError(
                    f"source[{i}].events[{j}] missing: {missing_evt}"
                )

    # Validate enrichment if present
    for i, enrichment in enumerate(el.get("enrichment", [])):
        for key in ("table", "join_key", "columns"):
            if key not in enrichment:
                raise ConfigValidationError(
                    f"enrichment[{i}] missing required key: {key}"
                )

    return el
