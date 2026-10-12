import csv
import json
import sys

import pytest

from measure_cli import append_measurement, logical_output_bytes, measure_command
from run_nyc_measurements import run_series, verify_report


def test_measure_python_and_child(tmp_path):
    output = tmp_path / "output"
    script = tmp_path / "work.py"
    script.write_text(
        "import subprocess, sys, time\n"
        "from pathlib import Path\n"
        "child = subprocess.Popen([sys.executable, '-c', "
        "'import time; data = bytearray(8000000); time.sleep(1)'])\n"
        "data = bytearray(8000000)\n"
        "Path('output').mkdir()\n"
        "Path('output/data.bin').write_bytes(b'abcde')\n"
        "print('completed', flush=True)\n"
        "child.wait()\n",
        encoding="utf-8",
    )
    row = measure_command([sys.executable, script], output, tmp_path / "cli.log", tmp_path, {"python": "test"})
    assert row["exit_code"] == 0
    assert float(row["elapsed_seconds"]) >= 1
    assert row["max_observed_process_count"] >= 2
    assert row["peak_sampled_tree_rss_bytes"] > 16_000_000
    assert row["sample_count"] >= 4
    assert row["sample_interval_ms"] == 200
    assert row["rss_access_denied_events"] == 0
    assert row["output_logical_bytes"] == 5
    assert "completed" in (tmp_path / "cli.log").read_text()
    csv_path = tmp_path / "results.csv"
    append_measurement(csv_path, row)
    append_measurement(csv_path, row)
    with csv_path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 2
    assert json.loads(rows[0]["command_json"]) == [sys.executable, str(script)]


def test_nonzero_exit_and_log_protection(tmp_path):
    log = tmp_path / "failure.log"
    row = measure_command([sys.executable, "-c", "print('failure'); raise SystemExit(7)"],
                          tmp_path / "missing", log, tmp_path, {})
    assert row["exit_code"] == 7
    assert row["output_logical_bytes"] == 0
    assert "failure" in log.read_text()
    with pytest.raises(FileExistsError):
        measure_command([sys.executable, "-c", "pass"], tmp_path, log, tmp_path, {})


def test_logical_size_and_symlink(tmp_path):
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested/file").write_bytes(b"123")
    (tmp_path / "empty").write_bytes(b"")
    assert logical_output_bytes(tmp_path) == 3
    try:
        (tmp_path / "link").symlink_to(tmp_path / "nested/file")
    except OSError:
        pytest.skip("Creating symlinks is not supported in this test environment")
    with pytest.raises(ValueError, match="symlinks"):
        logical_output_bytes(tmp_path)


def silver_report():
    return {
        "checks": {name: True for name in ["zone_identifiers_valid_unique", "zone_join_counts_preserved",
                   "rows_reconciled", "parquet_schemas_values_multiplicities_verified"]},
        "requested_month": "2025-01", "input_rows": 3, "clean_rows": 2, "quarantined_rows": 1,
        "read_back_rows": {"clean": 2, "quarantined": 1}, "join_counts": {"pickup": 3, "dropoff": 3},
        "quality_flags": {}, "rejection_reasons": {},
    }


def test_silver_quality_verification():
    report = silver_report()
    assert verify_report("Silver", report)["clean_rows"] == 2
    report["join_counts"]["pickup"] = 4
    with pytest.raises(ValueError, match="join counts"):
        verify_report("Silver", report)
    report = silver_report()
    report["checks"]["rows_reconciled"] = False
    with pytest.raises(ValueError, match="failed checks"):
        verify_report("Silver", report)


def test_gold_decimal_reconciliation(tmp_path):
    report = {
        "checks": {name: True for name in ["labels_consistent_per_key", "label_join_preserves_groups",
                   "keys_unique", "trip_counts_reconciled", "net_amount_reconciled", "group_amount_identity",
                   "group_counter_bounds", "read_back_schema_keys_contents_totals_verified"]},
        "silver_path": str(tmp_path), "input_rows": 2, "group_rows": 1, "read_back_groups": 1,
        "source_totals": {"trip_count": 2, "net_amount_total": "0.10"},
        "totals": {"trip_count": 2, "positive_amount_total": "0.30", "negative_amount_total": "-0.20",
                   "net_amount_total": "0.10", "zero_amount_trip_count": 0, "flagged_trip_count": 1},
    }
    assert verify_report("Gold", report, silver_report(), tmp_path)["group_rows"] == 1
    report["totals"]["net_amount_total"] = "0.11"
    with pytest.raises(ValueError, match="do not reconcile"):
        verify_report("Gold", report, silver_report(), tmp_path)
    report["totals"]["net_amount_total"] = 0.10
    with pytest.raises(ValueError, match="decimal strings"):
        verify_report("Gold", report, silver_report(), tmp_path)


def test_series_stops_on_failure(tmp_path, monkeypatch):
    import run_nyc_measurements as series

    calls = []
    monkeypatch.setattr(series, "runtime_versions", lambda: {})

    def failed_measure(command, output, log, cwd, versions):
        calls.append(command)
        return {"exit_code": 9, "log_path": str(log)}

    monkeypatch.setattr(series, "measure_command", failed_measure)
    csv_path = tmp_path / "results.csv"
    with pytest.raises(RuntimeError, match="Series stopped at Silver run 1"):
        run_series(tmp_path, "test", csv_path)
    assert len(calls) == 1
    with csv_path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 1
    assert rows[0]["quality_verified"] == "False"
    assert "code 9" in rows[0]["error"]


def test_series_requires_new_paths(tmp_path):
    output = tmp_path / "data/silver/test_run2"
    output.mkdir(parents=True)
    marker = output / "existing"
    marker.write_text("preserve")
    with pytest.raises(FileExistsError, match="new paths"):
        run_series(tmp_path, "test", tmp_path / "results.csv")
    assert marker.read_text() == "preserve"


@pytest.mark.parametrize("change_second_silver", [False, True])
def test_series_order_and_consistent_reports(tmp_path, monkeypatch, change_second_silver):
    import run_nyc_measurements as series

    calls = []
    monkeypatch.setattr(series, "runtime_versions", lambda: {})

    def successful_measure(command, output, log, cwd, versions):
        phase = "Silver" if command[1].endswith("run_nyc_silver.py") else "Gold"
        calls.append(phase)
        output.mkdir(parents=True)
        if phase == "Silver":
            report = silver_report()
            if change_second_silver and len(calls) == 3:
                report["quality_flags"] = {"zero_total_amount": 1}
        else:
            silver_path = command[command.index("--silver") + 1]
            assert silver_path == str(tmp_path / "data/silver" / output.name / "trips_clean")
            report = {
                "checks": {name: True for name in ["labels_consistent_per_key", "label_join_preserves_groups",
                           "keys_unique", "trip_counts_reconciled", "net_amount_reconciled", "group_amount_identity",
                           "group_counter_bounds", "read_back_schema_keys_contents_totals_verified"]},
                "silver_path": silver_path, "input_rows": 2, "group_rows": 1, "read_back_groups": 1,
                "source_totals": {"trip_count": 2, "net_amount_total": "3.00"},
                "totals": {"trip_count": 2, "positive_amount_total": "3.00", "negative_amount_total": "0.00",
                           "net_amount_total": "3.00", "zero_amount_trip_count": 0, "flagged_trip_count": 0},
            }
        report["spark"] = {"master": "local[2]"}
        (output / "quality_report.json").write_text(json.dumps(report))
        return {"exit_code": 0, "log_path": str(log), "rss_access_denied_events": 0,
                "java_observed": True, "elapsed_seconds": "1", "peak_sampled_tree_rss_bytes": 1,
                "output_logical_bytes": 1}

    monkeypatch.setattr(series, "measure_command", successful_measure)
    csv_path = tmp_path / "results.csv"
    if change_second_silver:
        with pytest.raises(RuntimeError, match="counters differ across runs"):
            run_series(tmp_path, "test", csv_path)
        assert calls == ["Silver", "Gold", "Silver"]
    else:
        run_series(tmp_path, "test", csv_path)
        assert calls == ["Silver", "Gold"] * 3
    with csv_path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == len(calls)
    assert rows[-1]["quality_verified"] == str(not change_second_silver)
