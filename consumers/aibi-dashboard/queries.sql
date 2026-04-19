-- =============================================================================
-- AI/BI Dashboard: Process Mining Executive View
-- =============================================================================
-- These queries power a Lakeview dashboard for executive process monitoring.
-- Each query maps to one widget in the dashboard.
-- =============================================================================

-- ── Widget 1: KPI Cards ──
-- Current month vs. prior month comparison

SELECT
    cases_started,
    ROUND(median_duration_days, 1) AS median_duration_days,
    ROUND(sla_breach_rate * 100, 1) AS sla_breach_pct,
    ROUND(happy_path_rate * 100, 1) AS happy_path_pct,
    ROUND(rework_rate * 100, 1) AS rework_pct,
    ROUND(avg_case_cost, 2) AS avg_case_cost
FROM process_mining.gold.process_kpis
ORDER BY month DESC
LIMIT 1;


-- ── Widget 2: Monthly Trend — Throughput & Duration ──

SELECT
    month,
    cases_started,
    ROUND(median_duration_days, 1) AS median_days,
    ROUND(p95_duration_days, 1) AS p95_days
FROM process_mining.gold.process_kpis
ORDER BY month;


-- ── Widget 3: Monthly Trend — SLA & Conformance ──

SELECT
    month,
    ROUND(sla_breach_rate * 100, 1) AS sla_breach_pct,
    ROUND(happy_path_rate * 100, 1) AS happy_path_pct,
    ROUND(rework_rate * 100, 1) AS rework_pct
FROM process_mining.gold.process_kpis
ORDER BY month;


-- ── Widget 4: Top 10 Process Variants ──

SELECT
    variant,
    COUNT(*) AS case_count,
    ROUND(AVG(case_duration_days), 1) AS avg_duration_days,
    ROUND(SUM(CASE WHEN sla_breach THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS sla_breach_pct
FROM process_mining.gold.case_summary
GROUP BY variant
ORDER BY case_count DESC
LIMIT 10;


-- ── Widget 5: Duration Distribution ──

SELECT
    CASE
        WHEN case_duration_days < 7 THEN '< 1 week'
        WHEN case_duration_days < 14 THEN '1-2 weeks'
        WHEN case_duration_days < 30 THEN '2-4 weeks'
        WHEN case_duration_days < 60 THEN '1-2 months'
        ELSE '2+ months'
    END AS duration_bucket,
    COUNT(*) AS case_count
FROM process_mining.gold.case_summary
GROUP BY 1
ORDER BY MIN(case_duration_days);


-- ── Widget 6: Bottleneck Transitions ──
-- Median time between consecutive activities

SELECT
    activity AS from_activity,
    next_activity AS to_activity,
    ROUND(PERCENTILE(time_to_next_seconds / 3600.0, 0.5), 1) AS median_hours,
    COUNT(*) AS transition_count
FROM (
    SELECT
        case_id,
        activity,
        LEAD(activity) OVER (PARTITION BY case_id ORDER BY event_timestamp) AS next_activity,
        LEAD(event_timestamp) OVER (PARTITION BY case_id ORDER BY event_timestamp)
            - event_timestamp AS time_to_next_seconds
    FROM process_mining.silver.event_log
)
WHERE next_activity IS NOT NULL
GROUP BY activity, next_activity
ORDER BY median_hours DESC
LIMIT 15;


-- ── Widget 7: SLA Breach by Source System ──

SELECT
    source_system,
    COUNT(*) AS total_cases,
    SUM(CASE WHEN sla_breach THEN 1 ELSE 0 END) AS breached,
    ROUND(SUM(CASE WHEN sla_breach THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS breach_pct
FROM process_mining.gold.case_summary
GROUP BY source_system
ORDER BY breach_pct DESC;


-- ── Widget 8: Non-Conforming Case Categories ──
-- (Requires Foundation Model API access)
-- Uncomment when ai_classify is available on your warehouse:
--
-- SELECT
--     ai_classify(
--         deviation_description,
--         ARRAY('rework_loop', 'skipped_approval', 'sla_breach_only', 'abandoned_process')
--     ) AS deviation_category,
--     COUNT(*) AS case_count,
--     ROUND(AVG(case_duration_days), 1) AS avg_duration
-- FROM process_mining.gold.non_conforming_cases
-- GROUP BY 1
-- ORDER BY case_count DESC;
