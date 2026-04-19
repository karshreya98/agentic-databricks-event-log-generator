"""
Event log quality validation.

Checks a DataFrame for common event log problems and reports them
as warnings or errors with suggested fixes.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from dataclasses import dataclass


@dataclass
class ValidationResult:
    check: str
    status: str  # "pass", "warn", "fail"
    message: str
    detail: str | None = None


class EventLogValidator:
    """
    Validate an event log DataFrame for process mining readiness.

    Usage:
        validator = EventLogValidator(event_log_df)
        results = validator.run_all()
        validator.report()
    """

    def __init__(
        self,
        df: DataFrame,
        case_id: str = "case_id",
        activity: str = "activity",
        timestamp: str = "event_timestamp",
    ):
        self.df = df
        self.case_id = case_id
        self.activity = activity
        self.timestamp = timestamp
        self._results: list[ValidationResult] = []

    def run_all(self) -> list[ValidationResult]:
        """Run all validation checks."""
        self._results = []
        self._check_required_columns()
        self._check_null_values()
        self._check_timestamp_ordering()
        self._check_duplicate_events()
        self._check_single_event_cases()
        self._check_activity_cardinality()
        self._check_case_completeness()
        return self._results

    def _add(self, check: str, status: str, message: str, detail: str = None):
        self._results.append(ValidationResult(check, status, message, detail))

    def _check_required_columns(self):
        """Check that required columns exist."""
        for col in (self.case_id, self.activity, self.timestamp):
            if col in self.df.columns:
                self._add("required_columns", "pass", f"Column '{col}' exists")
            else:
                self._add(
                    "required_columns", "fail",
                    f"Missing required column: '{col}'",
                    f"Available columns: {self.df.columns}",
                )

    def _check_null_values(self):
        """Check for nulls in key columns."""
        total = self.df.count()
        for col in (self.case_id, self.activity, self.timestamp):
            if col not in self.df.columns:
                continue
            nulls = self.df.filter(F.col(col).isNull()).count()
            if nulls == 0:
                self._add("null_check", "pass", f"No nulls in '{col}'")
            else:
                pct = nulls / total * 100
                self._add(
                    "null_check",
                    "fail" if pct > 5 else "warn",
                    f"{nulls:,} nulls in '{col}' ({pct:.1f}%)",
                    f"Fix: filter with .filter(F.col('{col}').isNotNull())",
                )

    def _check_timestamp_ordering(self):
        """Check for cases with out-of-order timestamps."""
        from pyspark.sql.window import Window
        w = Window.partitionBy(self.case_id).orderBy(self.timestamp)

        out_of_order = (
            self.df
            .withColumn("_prev_ts", F.lag(self.timestamp).over(w))
            .filter(F.col(self.timestamp) < F.col("_prev_ts"))
            .select(self.case_id)
            .distinct()
            .count()
        )

        total_cases = self.df.select(self.case_id).distinct().count()
        if out_of_order == 0:
            self._add("timestamp_order", "pass", "All cases have ordered timestamps")
        else:
            self._add(
                "timestamp_order", "warn",
                f"{out_of_order:,} cases have out-of-order timestamps "
                f"({out_of_order/total_cases*100:.1f}%)",
                "Possible causes: timezone issues, batch-loaded events with same timestamp",
            )

    def _check_duplicate_events(self):
        """Check for exact duplicate rows."""
        total = self.df.count()
        distinct = self.df.dropDuplicates().count()
        dupes = total - distinct

        if dupes == 0:
            self._add("duplicates", "pass", "No exact duplicate events")
        else:
            self._add(
                "duplicates", "warn",
                f"{dupes:,} duplicate events found ({dupes/total*100:.1f}%)",
                "Fix: .dropDuplicates() or deduplicate by event_id",
            )

    def _check_single_event_cases(self):
        """Check for cases with only one event (often noise)."""
        case_counts = (
            self.df.groupBy(self.case_id)
            .count()
            .filter(F.col("count") == 1)
            .count()
        )
        total_cases = self.df.select(self.case_id).distinct().count()

        if case_counts == 0:
            self._add("single_event_cases", "pass", "No single-event cases")
        elif case_counts / total_cases < 0.05:
            self._add(
                "single_event_cases", "pass",
                f"{case_counts:,} single-event cases ({case_counts/total_cases*100:.1f}%) — acceptable",
            )
        else:
            self._add(
                "single_event_cases", "warn",
                f"{case_counts:,} cases have only 1 event ({case_counts/total_cases*100:.1f}%)",
                "These add noise to process mining. Consider filtering them.",
            )

    def _check_activity_cardinality(self):
        """Check if activity count is reasonable."""
        n_activities = self.df.select(self.activity).distinct().count()

        if n_activities < 3:
            self._add(
                "activity_cardinality", "warn",
                f"Only {n_activities} distinct activities — may be too few for meaningful process mining",
            )
        elif n_activities > 200:
            self._add(
                "activity_cardinality", "warn",
                f"{n_activities} distinct activities — may need standardization",
                "High cardinality often means unstandardized names. "
                "Use activity_overrides in config or review with: "
                "df.groupBy('activity').count().orderBy('count')",
            )
        else:
            self._add(
                "activity_cardinality", "pass",
                f"{n_activities} distinct activities",
            )

    def _check_case_completeness(self):
        """Check the distribution of events per case."""
        stats = (
            self.df.groupBy(self.case_id)
            .count()
            .agg(
                F.avg("count").alias("avg_events"),
                F.expr("percentile(count, 0.5)").alias("median_events"),
                F.min("count").alias("min_events"),
                F.max("count").alias("max_events"),
            )
            .collect()[0]
        )

        self._add(
            "case_completeness", "pass",
            f"Events per case: min={stats['min_events']}, "
            f"median={stats['median_events']:.0f}, "
            f"avg={stats['avg_events']:.1f}, "
            f"max={stats['max_events']}",
        )

    def report(self) -> None:
        """Print a formatted validation report."""
        if not self._results:
            self.run_all()

        icons = {"pass": "[OK]", "warn": "[!!]", "fail": "[XX]"}
        passes = sum(1 for r in self._results if r.status == "pass")
        warns = sum(1 for r in self._results if r.status == "warn")
        fails = sum(1 for r in self._results if r.status == "fail")

        print(f"\n{'=' * 60}")
        print(f"  Event Log Validation Report")
        print(f"  {passes} passed, {warns} warnings, {fails} failures")
        print(f"{'=' * 60}")

        for r in self._results:
            print(f"\n  {icons[r.status]}  {r.check}")
            print(f"      {r.message}")
            if r.detail:
                print(f"      → {r.detail}")

        print(f"\n{'=' * 60}")
