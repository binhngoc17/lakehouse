import sys, os, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from run import load_pipelines, topological_sort, build_ctas_sql


def _write_pipeline(directory, name, yaml_body, sql_body):
    with open(os.path.join(directory, f"{name}.yaml"), "w") as f:
        f.write(yaml_body)
    with open(os.path.join(directory, f"{name}.sql"), "w") as f:
        f.write(sql_body)


def test_load_pipelines_reads_yaml_and_sql():
    with tempfile.TemporaryDirectory() as d:
        _write_pipeline(d, "my_table",
            "name: my_table\ntarget: gold.my_table\nstrategy: replace\ndepends_on: []\n",
            "SELECT 1 AS val")
        pipelines = load_pipelines(d)
    assert len(pipelines) == 1
    assert pipelines[0]["name"] == "my_table"
    assert pipelines[0]["sql"].strip() == "SELECT 1 AS val"
    assert pipelines[0]["target"] == "gold.my_table"


def test_load_pipelines_raises_when_sql_missing():
    import pytest
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "orphan.yaml"), "w") as f:
            f.write("name: orphan\ntarget: gold.orphan\nstrategy: replace\n")
        with pytest.raises(FileNotFoundError):
            load_pipelines(d)


def test_topological_sort_respects_gold_deps():
    pipelines = [
        {"name": "hvc", "target": "gold.hvc",
         "depends_on": ["gold.profile", "gold.spending"], "sql": ""},
        {"name": "spending", "target": "gold.spending",
         "depends_on": ["silver.transactions"], "sql": ""},
        {"name": "profile", "target": "gold.profile",
         "depends_on": ["silver.customers"], "sql": ""},
    ]
    ordered = topological_sort(pipelines)
    targets = [p["target"] for p in ordered]
    assert targets.index("gold.hvc") > targets.index("gold.profile")
    assert targets.index("gold.hvc") > targets.index("gold.spending")


def test_topological_sort_no_gold_deps_preserves_all():
    pipelines = [
        {"name": "a", "target": "gold.a", "depends_on": ["silver.x"], "sql": ""},
        {"name": "b", "target": "gold.b", "depends_on": ["silver.y"], "sql": ""},
    ]
    ordered = topological_sort(pipelines)
    assert len(ordered) == 2


def test_build_ctas_sql_replace():
    sql = build_ctas_sql("gold.customer_profile", "SELECT 1", strategy="replace")
    assert sql.startswith("CREATE OR REPLACE TABLE iceberg.gold.customer_profile AS")
    assert "SELECT 1" in sql


def test_build_ctas_sql_append():
    sql = build_ctas_sql("gold.spend", "SELECT 2", strategy="append")
    assert sql.startswith("INSERT INTO iceberg.gold.spend")
    assert "SELECT 2" in sql
