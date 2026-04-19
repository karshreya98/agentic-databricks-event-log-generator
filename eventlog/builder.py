"""
Configuration-driven event log builder.

Reads a YAML config and generates a Spark pipeline that:
1. Extracts events from each source table
2. Unions them into a single event log
3. Applies activity standardization
4. Computes event ordering and case metrics
5. Optionally applies enrichment joins
6. Writes the result to a Unity Catalog table
"""

import yaml
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from eventlog.schemas import validate_config, ConfigValidationError


class EventLogBuilder:
    """
    Build a process event log from a YAML config.

    Usage:
        builder = EventLogBuilder(spark, "templates/procure_to_pay.yaml")
        builder.build()            # preview the event log
        builder.save()             # write to the configured output table
        builder.summary()          # print stats
    """

    def __init__(self, spark: SparkSession, config_path: str):
        self.spark = spark
        self.config_path = config_path

        with open(config_path) as f:
            raw_config = yaml.safe_load(f)

        self.config = validate_config(raw_config)
        self._event_log: DataFrame | None = None

    @property
    def name(self) -> str:
        return self.config["name"]

    @property
    def output_table(self) -> str:
        return self.config["output_table"]

    def _extract_source(self, source: dict) -> DataFrame:
        """Extract events from a single source table."""
        table = source["table"]
        case_id_col = source["case_id"]
        timezone = source.get("timezone", "UTC")

        source_df = self.spark.table(table)
        event_frames = []

        for event in source["events"]:
            activity_name = event["activity"]
            ts_col = event["timestamp"]
            condition = event.get("condition")
            resource_col = event.get("resource")
            cost_col = event.get("cost")

            df = source_df.select(
                F.col(case_id_col).cast("string").alias("case_id"),
                F.lit(activity_name).alias("activity"),
                F.to_utc_timestamp(F.col(ts_col), timezone).alias("event_timestamp"),
                (F.col(resource_col).cast("string") if resource_col else F.lit(None)).alias("resource"),
                (F.col(cost_col).cast("double") if cost_col else F.lit(None).cast("double")).alias("cost"),
                F.lit(table).alias("source_table"),
            )

            if condition:
                df = df.filter(condition)

            # Drop rows where timestamp is null (event didn't happen)
            df = df.filter(F.col("event_timestamp").isNotNull())

            event_frames.append(df)

        from functools import reduce
        return reduce(DataFrame.unionByName, event_frames)

    def _apply_standardization(self, df: DataFrame) -> DataFrame:
        """Apply activity name overrides from config."""
        overrides = self.config.get("standardization", {}).get("activity_overrides", {})
        if not overrides:
            return df

        mapping_expr = F.create_map(
            [F.lit(x) for pair in overrides.items() for x in pair]
        )
        return df.withColumn(
            "activity",
            F.coalesce(mapping_expr[F.col("activity")], F.col("activity")),
        )

    def _add_case_metrics(self, df: DataFrame) -> DataFrame:
        """Add event ordering and timing within each case."""
        w = Window.partitionBy("case_id").orderBy("event_timestamp")

        return (
            df
            .withColumn("event_rank", F.row_number().over(w))
            .withColumn(
                "time_since_prev_seconds",
                F.col("event_timestamp").cast("long")
                - F.lag("event_timestamp").over(w).cast("long"),
            )
            .withColumn(
                "case_event_count",
                F.count("*").over(Window.partitionBy("case_id")),
            )
        )

    def _apply_enrichment(self, df: DataFrame) -> DataFrame:
        """Join enrichment tables defined in config."""
        for enrichment in self.config.get("enrichment", []):
            enrich_table = self.spark.table(enrichment["table"])
            join_key = enrichment["join_key"]
            columns = enrichment["columns"]

            # Select only the join key + requested columns to avoid ambiguity
            enrich_cols = [F.col(join_key)] + [F.col(c) for c in columns]
            enrich_subset = enrich_table.select(*enrich_cols).dropDuplicates([join_key])

            df = df.join(enrich_subset, on=join_key, how="left")

        return df

    def build(self) -> DataFrame:
        """Build the event log DataFrame (does not write)."""
        if self._event_log is not None:
            return self._event_log

        # Extract from all sources
        from functools import reduce
        source_dfs = [self._extract_source(s) for s in self.config["sources"]]
        unified = reduce(DataFrame.unionByName, source_dfs)

        # Standardize
        standardized = self._apply_standardization(unified)

        # Order and compute metrics
        with_metrics = self._add_case_metrics(standardized)

        # Enrich
        enriched = self._apply_enrichment(with_metrics)

        self._event_log = enriched
        return self._event_log

    def save(self, mode: str = "overwrite") -> None:
        """Build and save the event log to the configured output table."""
        df = self.build()

        # Create catalog/schema if needed
        parts = self.output_table.split(".")
        if len(parts) == 3:
            catalog, schema, _ = parts
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        (
            df.write
            .mode(mode)
            .option("overwriteSchema", "true")
            .saveAsTable(self.output_table)
        )

        print(f"Event log saved to {self.output_table}")
        self.summary()

    def summary(self) -> None:
        """Print summary statistics for the event log."""
        df = self.build()
        n_events = df.count()
        n_cases = df.select("case_id").distinct().count()
        n_activities = df.select("activity").distinct().count()
        sources = df.select("source_table").distinct().count()

        print(f"\n{'=' * 50}")
        print(f"  Event Log: {self.name}")
        print(f"{'=' * 50}")
        print(f"  Events:        {n_events:>10,}")
        print(f"  Cases:         {n_cases:>10,}")
        print(f"  Activities:    {n_activities:>10,}")
        print(f"  Source tables: {sources:>10,}")
        print(f"  Output:        {self.output_table}")
        print(f"{'=' * 50}\n")
