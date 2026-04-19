"""
Event log enrichment: discover and apply contextual data from the catalog.

Scans Unity Catalog for tables with joinable keys to the event log,
suggests enrichments, and optionally evaluates their impact on
downstream ML models.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from dataclasses import dataclass


@dataclass
class EnrichmentCandidate:
    """A candidate table + column set for enriching the event log."""
    table: str
    join_key: str
    columns: list[str]
    match_rate: float  # % of event log rows that have a match
    sample_values: dict  # column -> sample values for preview


class EventLogEnricher:
    """
    Discover and apply enrichments to an event log.

    Usage:
        enricher = EventLogEnricher(spark, event_log_df)
        candidates = enricher.discover(catalogs=["procurement", "finance"])
        enricher.preview(candidates[0])
        enriched_df = enricher.apply(candidates[:3])
    """

    def __init__(self, spark: SparkSession, event_log: DataFrame):
        self.spark = spark
        self.event_log = event_log

        # Detect potential join keys from the event log
        self._event_log_columns = set(event_log.columns)

    def discover(
        self,
        catalogs: list[str] | None = None,
        schemas: list[str] | None = None,
        min_match_rate: float = 0.1,
    ) -> list[EnrichmentCandidate]:
        """
        Scan the catalog for tables that can enrich the event log.

        Looks for tables with columns that share names with event log columns
        (potential join keys) and have non-trivial overlap.

        Args:
            catalogs: Catalogs to search. None = search all accessible.
            schemas: Schemas to filter (within catalogs). None = all.
            min_match_rate: Minimum % of event log rows that must match.

        Returns:
            List of EnrichmentCandidate, sorted by match rate descending.
        """
        candidates = []

        # Get list of tables to scan
        tables = self._list_candidate_tables(catalogs, schemas)

        # The event log's own table and common internal columns to skip
        skip_columns = {
            "event_timestamp", "event_rank", "time_since_prev_seconds",
            "case_event_count", "activity", "source_table", "cost", "resource",
        }

        # Potential join keys in the event log
        join_key_candidates = [
            c for c in self._event_log_columns
            if c not in skip_columns
            and c.endswith("_id") or c in ("case_id", "supplier_id", "customer_id", "contract_id", "order_id")
        ]

        for table_name in tables:
            try:
                candidate = self._evaluate_table(
                    table_name, join_key_candidates, skip_columns, min_match_rate
                )
                if candidate:
                    candidates.append(candidate)
            except Exception:
                # Skip tables we can't read (permissions, etc.)
                continue

        candidates.sort(key=lambda c: c.match_rate, reverse=True)
        return candidates

    def _list_candidate_tables(
        self, catalogs: list[str] | None, schemas: list[str] | None
    ) -> list[str]:
        """List tables from specified catalogs/schemas."""
        tables = []

        if catalogs is None:
            catalog_rows = self.spark.sql("SHOW CATALOGS").collect()
            catalogs = [r[0] for r in catalog_rows if r[0] not in ("system", "samples")]

        for catalog in catalogs:
            try:
                if schemas:
                    schema_list = schemas
                else:
                    schema_rows = self.spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
                    schema_list = [
                        r[0] for r in schema_rows
                        if r[0] not in ("information_schema", "default")
                    ]

                for schema in schema_list:
                    try:
                        table_rows = self.spark.sql(
                            f"SHOW TABLES IN {catalog}.{schema}"
                        ).collect()
                        for r in table_rows:
                            tables.append(f"{catalog}.{schema}.{r['tableName']}")
                    except Exception:
                        continue
            except Exception:
                continue

        return tables

    def _evaluate_table(
        self,
        table_name: str,
        join_key_candidates: list[str],
        skip_columns: set,
        min_match_rate: float,
    ) -> EnrichmentCandidate | None:
        """Check if a table is a good enrichment candidate."""
        table_df = self.spark.table(table_name)
        table_columns = set(table_df.columns)

        # Find matching join keys
        matching_keys = [k for k in join_key_candidates if k in table_columns]
        if not matching_keys:
            return None

        # Use the first matching key (prefer more specific ones)
        join_key = matching_keys[0]

        # Identify enrichment columns (columns NOT in the event log)
        enrichment_cols = [
            c for c in table_columns
            if c not in self._event_log_columns
            and c not in skip_columns
            and not c.startswith("_")
        ]

        if not enrichment_cols:
            return None

        # Calculate match rate
        event_log_keys = self.event_log.select(join_key).distinct()
        table_keys = table_df.select(join_key).distinct()
        matched = event_log_keys.join(table_keys, on=join_key, how="inner").count()
        total = event_log_keys.count()

        if total == 0:
            return None

        match_rate = matched / total

        if match_rate < min_match_rate:
            return None

        # Get sample values for preview
        sample_values = {}
        sample_row = table_df.select(enrichment_cols[:5]).limit(3).collect()
        for col in enrichment_cols[:5]:
            sample_values[col] = [str(getattr(r, col, None)) for r in sample_row]

        return EnrichmentCandidate(
            table=table_name,
            join_key=join_key,
            columns=enrichment_cols,
            match_rate=match_rate,
            sample_values=sample_values,
        )

    def preview(self, candidate: EnrichmentCandidate) -> None:
        """Print a preview of what an enrichment would add."""
        print(f"\n{'─' * 60}")
        print(f"  Enrichment: {candidate.table}")
        print(f"  Join key:   {candidate.join_key}")
        print(f"  Match rate: {candidate.match_rate:.1%}")
        print(f"  Columns:    {', '.join(candidate.columns[:10])}")
        if len(candidate.columns) > 10:
            print(f"              ... and {len(candidate.columns) - 10} more")
        print(f"\n  Sample values:")
        for col, vals in candidate.sample_values.items():
            print(f"    {col}: {vals}")
        print(f"{'─' * 60}")

    def apply(
        self, candidates: list[EnrichmentCandidate], how: str = "left"
    ) -> DataFrame:
        """
        Apply enrichment joins to the event log.

        Args:
            candidates: List of EnrichmentCandidate to join.
            how: Join type (default "left" to preserve all event log rows).

        Returns:
            Enriched DataFrame.
        """
        df = self.event_log

        for candidate in candidates:
            enrich_df = self.spark.table(candidate.table)
            enrich_cols = [F.col(candidate.join_key)] + [
                F.col(c) for c in candidate.columns
            ]
            enrich_subset = (
                enrich_df
                .select(*enrich_cols)
                .dropDuplicates([candidate.join_key])
            )

            df = df.join(enrich_subset, on=candidate.join_key, how=how)

            print(f"  + Joined {candidate.table} on {candidate.join_key} "
                  f"({candidate.match_rate:.0%} match, +{len(candidate.columns)} columns)")

        return df
