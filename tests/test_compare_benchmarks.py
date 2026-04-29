import csv
import importlib.util
import json
import sys
from pathlib import Path


def _load_module():
    module_path = Path(__file__).parents[1] / "tools" / "compare_benchmarks.py"
    spec = importlib.util.spec_from_file_location("compare_benchmarks", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _benchmark(name, mean, median=None, samples=None):
    stats = {
        "min": mean * 0.8,
        "max": mean * 1.2,
        "mean": mean,
        "median": median if median is not None else mean,
        "stddev": mean * 0.05,
        "iqr": mean * 0.1,
        "ops": 1 / mean,
        "rounds": len(samples or [mean]),
        "iterations": 1,
        "outliers": "0;0",
    }
    if samples is not None:
        stats["data"] = samples
    return {
        "group": name,
        "name": name.rsplit("::", 1)[-1],
        "fullname": name,
        "params": {},
        "param": None,
        "extra_info": {},
        "options": {},
        "stats": stats,
    }


def _write_run(path, benchmarks, branch):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "machine_info": {"node": "test"},
                "commit_info": {"branch": branch, "id": branch},
                "benchmarks": benchmarks,
                "datetime": "2026-04-29T08:00:00",
                "version": "5.2.3",
            }
        ),
        encoding="utf-8",
    )


def test_find_run_uses_latest_matching_saved_name(tmp_path):
    compare_benchmarks = _load_module()
    storage = tmp_path / ".benchmarks" / "Linux-CPython-3.12-64bit"
    _write_run(storage / "0001_main.json", [_benchmark("test_a", 1.0)], "main")
    _write_run(storage / "0003_main.json", [_benchmark("test_a", 0.9)], "main")

    run_path = compare_benchmarks.find_run_file(tmp_path / ".benchmarks", "main")

    assert run_path.name == "0003_main.json"


def test_compare_records_include_delta_status_and_sorting():
    compare_benchmarks = _load_module()
    base = compare_benchmarks.BenchmarkRun(
        label="main",
        path=Path("main.json"),
        data={},
        benchmarks={
            "test_fast": _benchmark("test_fast", 1.0),
            "test_slow": _benchmark("test_slow", 1.0),
        },
    )
    compare = compare_benchmarks.BenchmarkRun(
        label="feature",
        path=Path("feature.json"),
        data={},
        benchmarks={
            "test_fast": _benchmark("test_fast", 0.5),
            "test_slow": _benchmark("test_slow", 1.25),
        },
    )

    records = compare_benchmarks.compare_runs(base, compare, metric="mean", tolerance_percent=0)
    records = compare_benchmarks.sort_records(records, sort_by="relative-delta", sort_order="desc")

    assert [record.name for record in records] == ["test_slow", "test_fast"]
    assert records[0].status == "slower"
    assert records[0].absolute_delta == 0.25
    assert records[0].relative_delta == 25.0
    assert records[1].status == "faster"
    assert records[1].relative_delta == -50.0


def test_sorting_keeps_missing_values_last():
    compare_benchmarks = _load_module()
    base = compare_benchmarks.BenchmarkRun(
        label="main",
        path=Path("main.json"),
        data={},
        benchmarks={
            "test_common": _benchmark("test_common", 1.0),
            "test_missing": _benchmark("test_missing", 1.0),
        },
    )
    compare = compare_benchmarks.BenchmarkRun(
        label="feature",
        path=Path("feature.json"),
        data={},
        benchmarks={
            "test_common": _benchmark("test_common", 1.2),
        },
    )

    records = compare_benchmarks.compare_runs(base, compare, metric="mean", tolerance_percent=0)
    records = compare_benchmarks.sort_records(records, sort_by="relative-delta", sort_order="desc")

    assert [record.name for record in records] == ["test_common", "test_missing"]


def test_cli_exports_tables_and_svg_charts(tmp_path):
    compare_benchmarks = _load_module()
    storage = tmp_path / ".benchmarks" / "Linux-CPython-3.12-64bit"
    _write_run(
        storage / "0001_main.json",
        [
            _benchmark("tests/test_mod.py::test_a", 1.0, samples=[0.9, 1.0, 1.1]),
            _benchmark("tests/test_mod.py::test_b", 2.0),
        ],
        "main",
    )
    _write_run(
        storage / "0002_feature.json",
        [
            _benchmark("tests/test_mod.py::test_a", 1.5, samples=[1.4, 1.5, 1.6]),
            _benchmark("tests/test_mod.py::test_b", 1.0),
        ],
        "feature",
    )
    output = tmp_path / "comparison"

    exit_code = compare_benchmarks.main(
        [
            "--storage",
            str(tmp_path / ".benchmarks"),
            "--base",
            "main",
            "--compare",
            "feature",
            "--output",
            str(output),
            "--sort-by",
            "name",
            "--sort-order",
            "asc",
        ]
    )

    assert exit_code == 0
    assert (output / "comparison.csv").exists()
    assert (output / "comparison.json").exists()
    assert (output / "comparison.md").exists()
    assert len(list((output / "charts").glob("*.svg"))) == 2

    with (output / "comparison.csv").open(newline="", encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))

    assert rows[0]["name"] == "tests/test_mod.py::test_a"
    assert rows[0]["status"] == "slower"
    assert rows[1]["status"] == "faster"
