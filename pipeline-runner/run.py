import os
import sys
import time
import glob
import yaml
import requests
from collections import defaultdict, deque

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
PIPELINES_DIR = os.getenv("PIPELINES_DIR", "/pipelines")


def load_pipelines(directory: str) -> list[dict]:
    pipelines = []
    for yaml_path in sorted(glob.glob(os.path.join(directory, "*.yaml"))):
        sql_path = yaml_path.replace(".yaml", ".sql")
        if not os.path.exists(sql_path):
            raise FileNotFoundError(f"Missing SQL for {yaml_path}")
        with open(yaml_path) as f:
            config = yaml.safe_load(f)
        with open(sql_path) as f:
            config["sql"] = f.read()
        config.setdefault("depends_on", [])
        config.setdefault("strategy", "replace")
        pipelines.append(config)
    return pipelines


def topological_sort(pipelines: list[dict]) -> list[dict]:
    gold_targets = {p["target"] for p in pipelines}
    by_target = {p["target"]: p for p in pipelines}

    successors: dict[str, set[str]] = defaultdict(set)
    in_degree: dict[str, int] = {p["target"]: 0 for p in pipelines}

    for p in pipelines:
        for dep in p.get("depends_on", []):
            if dep in gold_targets:
                successors[dep].add(p["target"])
                in_degree[p["target"]] += 1

    queue = deque(p for p in pipelines if in_degree[p["target"]] == 0)
    ordered = []
    while queue:
        node = queue.popleft()
        ordered.append(node)
        for succ_target in successors[node["target"]]:
            in_degree[succ_target] -= 1
            if in_degree[succ_target] == 0:
                queue.append(by_target[succ_target])

    return ordered if len(ordered) == len(pipelines) else pipelines


def build_ctas_sql(target: str, sql: str, strategy: str = "replace") -> str:
    full_target = f"iceberg.{target}" if not target.startswith("iceberg.") else target
    body = sql.strip()
    if strategy == "replace":
        return f"CREATE OR REPLACE TABLE {full_target} AS\n{body}"
    return f"INSERT INTO {full_target}\n{body}"


def _trino_headers() -> dict:
    return {
        "X-Trino-User": "admin",
        "Content-Type": "text/plain",
    }


def wait_for_trino(retries: int = 40, delay: int = 5) -> None:
    url = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/info"
    for i in range(retries):
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200 and not r.json().get("starting", True):
                print("Trino ready")
                return
        except Exception:
            pass
        print(f"Waiting for Trino ({i + 1}/{retries})...")
        time.sleep(delay)
    raise RuntimeError("Trino did not become ready")


def run_trino_sql(sql: str) -> dict:
    url = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/statement"
    r = requests.post(url, data=sql, headers=_trino_headers(), timeout=30)
    r.raise_for_status()
    result = r.json()

    while "nextUri" in result:
        time.sleep(0.3)
        r = requests.get(result["nextUri"], headers=_trino_headers(), timeout=30)
        r.raise_for_status()
        result = r.json()

    if "error" in result:
        raise RuntimeError(f"Trino error: {result['error']['message']}")
    return result


def ensure_namespace(namespace: str) -> None:
    try:
        run_trino_sql(f"CREATE SCHEMA IF NOT EXISTS iceberg.{namespace}")
    except Exception as e:
        print(f"  warn: schema creation: {e}")


def run_pipeline(pipeline: dict) -> None:
    target = pipeline["target"]
    namespace = target.split(".")[0]
    ensure_namespace(namespace)

    full_sql = build_ctas_sql(target, pipeline["sql"], pipeline.get("strategy", "replace"))
    print(f"  > {pipeline['name']} -> iceberg.{target}")
    start = time.time()
    run_trino_sql(full_sql)
    elapsed = time.time() - start

    count = run_trino_sql(f"SELECT COUNT(*) FROM iceberg.{target}")
    rows = count.get("data", [[0]])[0][0] if count.get("data") else "?"
    print(f"    ok {rows} rows in {elapsed:.1f}s")


def main() -> None:
    wait_for_trino()
    pipelines = load_pipelines(PIPELINES_DIR)
    ordered = topological_sort(pipelines)
    print(f"Running {len(ordered)} pipelines...")
    for p in ordered:
        run_pipeline(p)
    print("All pipelines complete.")


if __name__ == "__main__":
    main()
