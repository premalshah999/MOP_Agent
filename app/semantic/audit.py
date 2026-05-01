from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any

from app.paths import MANIFEST_PATH, METADATA_PATH
from app.semantic.registry import load_registry, quote_identifier


@dataclass
class AuditIssue:
    severity: str
    code: str
    message: str
    dataset: str | None = None


@dataclass
class DatasetCoverage:
    dataset_id: str
    family: str
    geography: str
    runtime_column_count: int
    documented_column_count: int
    metric_count: int
    dimension_count: int
    metric_column_count: int
    dimension_column_count: int
    covered_column_count: int
    uncovered_columns: list[str] = field(default_factory=list)
    columns_missing_from_metadata: list[str] = field(default_factory=list)
    documented_columns_not_loaded: list[str] = field(default_factory=list)
    metrics_missing_synonyms: list[str] = field(default_factory=list)
    metrics_missing_semantic_variant: list[str] = field(default_factory=list)
    metric_variant_groups: dict[str, dict[str, str]] = field(default_factory=dict)


def _load_json(path) -> dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def _sql_references_column(sql: str, column: str) -> bool:
    quoted = quote_identifier(column)
    if quoted in sql:
        return True
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column):
        return bool(re.search(rf"\b{re.escape(column)}\b", sql))
    return False


def build_semantic_coverage_audit() -> dict[str, Any]:
    manifest = _load_json(MANIFEST_PATH)
    metadata = _load_json(METADATA_PATH)
    registry = load_registry()
    metadata_tables = metadata.get("tables", {})

    runtime_tables = set(manifest)
    documented_tables = set(metadata_tables)
    documented_not_loaded = sorted(documented_tables - runtime_tables)
    loaded_not_documented = sorted(runtime_tables - documented_tables)

    datasets: list[DatasetCoverage] = []
    issues: list[AuditIssue] = []
    for dataset_id, dataset in sorted(registry.datasets.items()):
        runtime_columns = list(manifest.get(dataset_id, {}).get("columns", []))
        documented_columns = set((metadata_tables.get(dataset_id) or {}).get("columns", {}))
        dimension_columns = {definition.column for definition in dataset.dimensions.values()}
        metric_columns = {
            column
            for column in runtime_columns
            if any(_sql_references_column(metric.sql, column) for metric in dataset.metrics.values())
        }
        structural_columns = {dataset.label_column}
        if dataset.year_column:
            structural_columns.add(dataset.year_column)
        covered_columns = dimension_columns | metric_columns | structural_columns
        uncovered_columns = sorted(set(runtime_columns) - covered_columns)

        metrics_missing_synonyms = sorted(
            metric.id
            for metric in dataset.metrics.values()
            if not metric.synonyms and not metric.default_for
        )
        metrics_missing_semantic_variant = sorted(
            metric.id
            for metric in dataset.metrics.values()
            if not metric.semantic_concept or not metric.semantic_variant
        )
        variant_groups: dict[str, dict[str, str]] = {}
        for metric in dataset.metrics.values():
            if metric.semantic_concept and metric.related_variants:
                variant_groups[metric.semantic_concept] = dict(sorted(metric.related_variants.items()))

        coverage = DatasetCoverage(
            dataset_id=dataset_id,
            family=dataset.family,
            geography=dataset.geography,
            runtime_column_count=len(runtime_columns),
            documented_column_count=len(documented_columns),
            metric_count=len(dataset.metrics),
            dimension_count=len(dataset.dimensions),
            metric_column_count=len(metric_columns),
            dimension_column_count=len(dimension_columns),
            covered_column_count=len(covered_columns & set(runtime_columns)),
            uncovered_columns=uncovered_columns,
            columns_missing_from_metadata=sorted(set(runtime_columns) - documented_columns),
            documented_columns_not_loaded=sorted(documented_columns - set(runtime_columns)),
            metrics_missing_synonyms=metrics_missing_synonyms,
            metrics_missing_semantic_variant=metrics_missing_semantic_variant,
            metric_variant_groups=variant_groups,
        )
        datasets.append(coverage)

        if not dataset.metrics:
            issues.append(AuditIssue("critical", "NO_METRICS", "Runtime dataset has no registered metrics.", dataset_id))
        if coverage.columns_missing_from_metadata:
            issues.append(
                AuditIssue(
                    "warning",
                    "RUNTIME_COLUMNS_MISSING_METADATA",
                    f"{len(coverage.columns_missing_from_metadata)} runtime columns are not documented in semantic metadata.",
                    dataset_id,
                )
            )
        if coverage.documented_columns_not_loaded:
            issues.append(
                AuditIssue(
                    "warning",
                    "DOCUMENTED_COLUMNS_NOT_LOADED",
                    f"{len(coverage.documented_columns_not_loaded)} documented columns are not loaded at runtime.",
                    dataset_id,
                )
            )
        if coverage.metrics_missing_synonyms:
            issues.append(
                AuditIssue(
                    "warning",
                    "METRICS_MISSING_SYNONYMS",
                    f"{len(coverage.metrics_missing_synonyms)} metrics have no synonyms/default aliases.",
                    dataset_id,
                )
            )
        if coverage.metrics_missing_semantic_variant:
            issues.append(
                AuditIssue(
                    "warning",
                    "METRICS_MISSING_VARIANTS",
                    f"{len(coverage.metrics_missing_semantic_variant)} metrics lack semantic concept/variant metadata.",
                    dataset_id,
                )
            )

    for table in documented_not_loaded:
        issues.append(AuditIssue("warning", "DOCUMENTED_TABLE_NOT_LOADED", "Semantic metadata documents a table absent from runtime manifest.", table))
    for table in loaded_not_documented:
        issues.append(AuditIssue("warning", "LOADED_TABLE_NOT_DOCUMENTED", "Runtime manifest loads a table absent from semantic metadata.", table))

    summary = {
        "runtime_table_count": len(runtime_tables),
        "documented_table_count": len(documented_tables),
        "registered_dataset_count": len(registry.datasets),
        "documented_not_loaded_count": len(documented_not_loaded),
        "loaded_not_documented_count": len(loaded_not_documented),
        "critical_issue_count": sum(1 for issue in issues if issue.severity == "critical"),
        "warning_count": sum(1 for issue in issues if issue.severity == "warning"),
        "metric_count": sum(item.metric_count for item in datasets),
        "dimension_count": sum(item.dimension_count for item in datasets),
        "runtime_column_count": sum(item.runtime_column_count for item in datasets),
        "covered_column_count": sum(item.covered_column_count for item in datasets),
    }
    summary["column_coverage_ratio"] = (
        round(summary["covered_column_count"] / summary["runtime_column_count"], 4)
        if summary["runtime_column_count"]
        else 1.0
    )

    return {
        "summary": summary,
        "documented_not_loaded": documented_not_loaded,
        "loaded_not_documented": loaded_not_documented,
        "datasets": [asdict(item) for item in datasets],
        "issues": [asdict(issue) for issue in issues],
    }


def audit_to_markdown(audit: dict[str, Any]) -> str:
    summary = audit["summary"]
    lines = [
        "# Semantic Coverage Audit",
        "",
        "## Summary",
        "",
        f"- Runtime tables: {summary['runtime_table_count']}",
        f"- Documented tables: {summary['documented_table_count']}",
        f"- Registered metrics: {summary['metric_count']}",
        f"- Registered dimensions: {summary['dimension_count']}",
        f"- Runtime column coverage: {summary['covered_column_count']}/{summary['runtime_column_count']} ({summary['column_coverage_ratio']:.1%})",
        f"- Critical issues: {summary['critical_issue_count']}",
        f"- Warnings: {summary['warning_count']}",
        "",
    ]
    if audit["documented_not_loaded"]:
        lines.extend(["## Documented But Not Loaded", ""])
        lines.extend(f"- `{table}`" for table in audit["documented_not_loaded"])
        lines.append("")

    lines.extend(["## Dataset Coverage", ""])
    lines.append("| Dataset | Metrics | Dimensions | Columns Covered | Uncovered Columns |")
    lines.append("|---|---:|---:|---:|---:|")
    for dataset in audit["datasets"]:
        lines.append(
            "| {dataset_id} | {metric_count} | {dimension_count} | {covered_column_count}/{runtime_column_count} | {uncovered} |".format(
                dataset_id=dataset["dataset_id"],
                metric_count=dataset["metric_count"],
                dimension_count=dataset["dimension_count"],
                covered_column_count=dataset["covered_column_count"],
                runtime_column_count=dataset["runtime_column_count"],
                uncovered=len(dataset["uncovered_columns"]),
            )
        )
    lines.append("")

    warnings = [issue for issue in audit["issues"] if issue["severity"] != "critical"]
    if warnings:
        lines.extend(["## Warnings", ""])
        for issue in warnings:
            dataset = f" `{issue['dataset']}`:" if issue.get("dataset") else ""
            lines.append(f"- `{issue['code']}`{dataset} {issue['message']}")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit runtime schema, semantic metadata, and registry coverage.")
    parser.add_argument("--format", choices=["json", "markdown"], default="json")
    args = parser.parse_args()
    audit = build_semantic_coverage_audit()
    if args.format == "markdown":
        print(audit_to_markdown(audit), end="")
    else:
        print(json.dumps(audit, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
