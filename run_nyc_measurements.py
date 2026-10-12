"""Measure three sequential Silver/Gold NYC pairs and check each quality report."""

import argparse
import json
import re
import sys
from decimal import Decimal, localcontext
from pathlib import Path

from measure_cli import append_measurement, measure_command, runtime_versions


def verify_report(phase, report, silver_report=None, silver_path=None):
    required_checks = {
        "Silver": ["zone_identifiers_valid_unique", "zone_join_counts_preserved", "rows_reconciled",
                   "parquet_schemas_values_multiplicities_verified"],
        "Gold": ["labels_consistent_per_key", "label_join_preserves_groups", "keys_unique",
                 "trip_counts_reconciled", "net_amount_reconciled", "group_amount_identity",
                 "group_counter_bounds", "read_back_schema_keys_contents_totals_verified"],
    }
    if any(report["checks"].get(name) is not True for name in required_checks[phase]):
        raise ValueError(f"{phase} report has missing or failed checks")
    if phase == "Silver":
        if report["requested_month"] != "2025-01":
            raise ValueError("Silver report month differs from January 2025")
        if report["input_rows"] != report["clean_rows"] + report["quarantined_rows"]:
            raise ValueError("Silver rows do not reconcile")
        if report["read_back_rows"] != {"clean": report["clean_rows"], "quarantined": report["quarantined_rows"]}:
            raise ValueError("Silver read-back counts differ")
        if any(count != report["input_rows"] for count in report["join_counts"].values()):
            raise ValueError("Silver zone join counts differ")
        return {key: report[key] for key in ["input_rows", "clean_rows", "quarantined_rows", "rejection_reasons", "quality_flags"]}
    totals = report["totals"]
    if Path(report["silver_path"]).resolve() != silver_path.resolve():
        raise ValueError("Gold did not read the corresponding Silver directory")
    if report["input_rows"] != silver_report["clean_rows"] or totals["trip_count"] != report["input_rows"]:
        raise ValueError("Gold count differs from corresponding Silver")
    if report["read_back_groups"] != report["group_rows"] or report["source_totals"]["trip_count"] != totals["trip_count"]:
        raise ValueError("Gold group or source counts differ")
    amounts = [totals[name] for name in ["positive_amount_total", "negative_amount_total", "net_amount_total"]]
    if any(not isinstance(value, str) for value in amounts):
        raise ValueError("Gold report monetary values must be decimal strings")
    positive, negative, net = map(Decimal, amounts)
    with localcontext() as context:
        context.prec = 50
        if positive + negative != net or positive < 0 or negative > 0:
            raise ValueError("Gold recorded amounts do not reconcile")
    if totals["net_amount_total"] != report["source_totals"]["net_amount_total"]:
        raise ValueError("Gold net amount differs from Silver report source totals")
    if any(not 0 <= totals[name] <= totals["trip_count"] for name in ["zero_amount_trip_count", "flagged_trip_count"]):
        raise ValueError("Gold counters are outside bounds")
    return {"input_rows": report["input_rows"], "group_rows": report["group_rows"], "totals": totals}


def run_series(project, tag, csv_path):
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", tag):
        raise ValueError("Tag must contain only letters, digits, underscores or hyphens")
    csv_path = csv_path.resolve()
    logs = project / "artifacts" / "cli-measurements" / tag
    targets = {phase: [project / "data" / directory / f"{tag}_run{run}" for run in range(1, 4)]
               for phase, directory in [("Silver", "silver"), ("Gold", "gold")]}
    for path in [csv_path, logs, *targets["Silver"], *targets["Gold"]]:
        if path.exists():
            raise FileExistsError(f"Measurement series requires new paths: {path}")
    logs.mkdir(parents=True)
    versions = runtime_versions()
    baseline = {}
    raw = project / "data/raw/nyc/yellow_tripdata_2025-01.parquet"
    zones = project / "data/raw/nyc/taxi_zone_lookup.csv"
    for index in range(3):
        silver_output, gold_output = targets["Silver"][index], targets["Gold"][index]
        silver_path = silver_output / "trips_clean"
        silver_report = None
        commands = {
            "Silver": [sys.executable, str(project / "run_nyc_silver.py"), "--input", str(raw),
                       "--zones", str(zones), "--month", "2025-01", "--output", str(silver_output)],
            "Gold": [sys.executable, str(project / "run_nyc_gold.py"), "--silver", str(silver_path), "--output", str(gold_output)],
        }
        for phase, output in [("Silver", silver_output), ("Gold", gold_output)]:
            print(f"Starting {phase} run {index + 1}/3: {output}", flush=True)
            row = measure_command(commands[phase], output, logs / f"{phase.lower()}_run{index + 1}.log", project, versions)
            row.update({"phase": phase, "run": index + 1, "month": "2025-01", "series_tag": tag,
                        "input_path": str(raw if phase == "Silver" else silver_path),
                        "zones_path": str(zones) if phase == "Silver" else "",
                        "report_path": str(output / "quality_report.json"), "storage_root": "/mnt/c" if str(project).startswith("/mnt/c/") else str(project.anchor),
                        "spark_json": "", "quality_summary_json": "", "quality_verified": False, "error": ""})
            try:
                if row["exit_code"] != 0:
                    raise RuntimeError(f"CLI exited with code {row['exit_code']}; see {row['log_path']}")
                if row["rss_access_denied_events"] or not row["java_observed"]:
                    raise RuntimeError("RSS measurement is incomplete or did not observe the JVM")
                report = json.loads((output / "quality_report.json").read_text(encoding="utf-8"))
                row["spark_json"] = json.dumps(report["spark"], sort_keys=True)
                signature = verify_report(phase, report, silver_report, silver_path)
                if phase in baseline and signature != baseline[phase]:
                    raise ValueError(f"{phase} quality counters differ across runs")
                baseline[phase] = signature
                row["quality_summary_json"] = json.dumps(signature, sort_keys=True)
                row["quality_verified"] = True
                if phase == "Silver":
                    silver_report = report
            except Exception as error:
                row["error"] = str(error)
                append_measurement(csv_path, row)
                raise RuntimeError(f"Series stopped at {phase} run {index + 1}: {error}") from error
            append_measurement(csv_path, row)
            print(f"Verified {phase} run {index + 1}: {row['elapsed_seconds']} s, "
                  f"sampled peak RSS {row['peak_sampled_tree_rss_bytes']} bytes, "
                  f"output {row['output_logical_bytes']} bytes", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--csv", type=Path, required=True)
    args = parser.parse_args()
    run_series(Path(__file__).resolve().parent, args.tag, args.csv)


if __name__ == "__main__":
    main()
