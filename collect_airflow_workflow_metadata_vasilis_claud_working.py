#!/usr/bin/env python3
import argparse
import json
import os
import re
import shlex
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

from collect_public_metadata_vasilis import (
    BASE_URI_DEFAULT,
    CARBON_INTENSITY_DEFAULT,
    ONTOLOGY_URI_DEFAULT,
    PROM_URL_DEFAULT,
    as_ttl_uri,
    http_get_json,
    joules_to_kwh,
    kubectl_json,
    parse_ts,
    run_cmd,
    sha256_file,
    slugify,
    ttl_literal,
)


AIRFLOW_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
POD_NAME_PATTERNS = [
    re.compile(r"Building pod ([a-z0-9-]+)"),
    re.compile(r"Found matching pod ([a-z0-9-]+)"),
    re.compile(r"Pod ([a-z0-9-]+) has phase"),
]
IMAGE_PATTERN = re.compile(r'image\s*=\s*["\']([^"\']+)["\']')
OVERHEAD_TASK_IDS = {
    "generate_allowed_tiles",
    "generate_analysis_mask",
    "prepare_level2",
    "prepare_tsa",
    "wait_for_trends",
}
BERLIN_TZ = ZoneInfo("Europe/Berlin")
GPU_RESOURCE_REGEX = re.compile(r'["\'](nvidia\.com/gpu|amd\.com/gpu|gpu)["\']\s*:')
SLURM_HINTS = ("slurm", "sbatch", "srun", "squeue")


def normalize_state(state: Optional[str]) -> str:
    mapping = {
        "success": "Succeeded",
        "failed": "Failed",
        "running": "Running",
        "queued": "Queued",
        "scheduled": "Scheduled",
        "skipped": "Skipped",
        "upstream_failed": "UpstreamFailed",
        "shutdown": "Shutdown",
        "removed": "Removed",
        "restarting": "Restarting",
        "deferred": "Deferred",
        "none": "None",
    }
    key = (state or "None").strip().lower()
    return mapping.get(key, state or "None")


def split_csv(value: str) -> List[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def looks_like_publication_uri(uri: Optional[str]) -> bool:
    if not uri:
        return False
    low = uri.lower()
    return "publication" in low or "doi-" in low or "/individual?" in low


def seconds_between(start: datetime, end: datetime) -> int:
    return max(1, int((end - start).total_seconds()))


def prom_at(ts: datetime) -> str:
    return f" @ {int(ts.timestamp())}"


def build_window(start: datetime, end: datetime) -> str:
    return f"[{seconds_between(start, end)}s]"


def prom_query(prom_url: str, query: str) -> Any:
    data = http_get_json(f"{prom_url}/api/v1/query", {"query": query})
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed:\n{query}\n{data}")
    return data["data"]["result"]


def ensure_prometheus_reachable(prom_url: str) -> None:
    try:
        prom_query(prom_url, "1")
    except Exception as exc:
        raise RuntimeError(
            f"Prometheus is not reachable at {prom_url}. "
            "Start or keep the monitoring port-forward running before collecting workflow metadata."
        ) from exc


def prom_metric_names(prom_url: str) -> List[str]:
    data = http_get_json(f"{prom_url}/api/v1/label/__name__/values")
    if data.get("status") != "success":
        return []
    return data.get("data", [])


def find_energy_metric(metric_names: List[str]) -> Optional[str]:
    preferred = [
        "kepler_container_joules_total",
        "kepler_pod_joules_total",
        "kepler_container_package_joules_total",
        "kepler_container_core_joules_total",
        "kepler_container_dram_joules_total",
        "kepler_pod_package_joules_total",
    ]
    for name in preferred:
        if name in metric_names:
            return name

    for name in metric_names:
        low = name.lower()
        if any(token in low for token in ["kepler", "joule", "energy", "power", "watt"]):
            return name
    return None


def has_gpu_metric_support(metric_names: List[str]) -> bool:
    return any(
        any(token in metric_name.lower() for token in ("gpu", "nvidia", "cuda", "dcgm"))
        for metric_name in metric_names
    )


def bytes_to_gb(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return float(value) / 1_000_000_000.0


def naive_local_iso(dt: datetime) -> str:
    return dt.replace(microsecond=0, tzinfo=None).isoformat()


def build_workflow_ui_link(base_url: Optional[str], dag_id: str) -> Optional[str]:
    if not base_url:
        return None
    return f"{base_url.rstrip('/')}/dags/{quote(dag_id, safe='')}/grid"


def parse_k8s_bytes(quantity: str) -> Optional[float]:
    quantity = (quantity or "").strip()
    if not quantity:
        return None
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([A-Za-z]+)?", quantity)
    if not match:
        return None
    value = float(match.group(1))
    suffix = (match.group(2) or "").strip()
    multipliers = {
        "": 1.0,
        "Ki": 1024.0,
        "Mi": 1024.0 ** 2,
        "Gi": 1024.0 ** 3,
        "Ti": 1024.0 ** 4,
        "Pi": 1024.0 ** 5,
        "Ei": 1024.0 ** 6,
        "K": 1000.0,
        "M": 1000.0 ** 2,
        "G": 1000.0 ** 3,
        "T": 1000.0 ** 4,
        "P": 1000.0 ** 5,
        "E": 1000.0 ** 6,
    }
    multiplier = multipliers.get(suffix)
    if multiplier is None:
        return None
    return value * multiplier


def ttl_bool(value: bool) -> str:
    return ttl_literal("true" if value else "false", "xsd:boolean")


def detect_backend(
    pod_names: Sequence[str],
    node_infos: Sequence[Dict[str, Any]],
    hint_texts: Sequence[str],
) -> str:
    if pod_names or node_infos:
        return "Kubernetes"
    merged_hints = " ".join(hint_texts).lower()
    if any(token in merged_hints for token in SLURM_HINTS):
        return "SLURM"
    return "Local"


def summarize_energy_method(
    detected_energy_metric: Optional[str],
    energy_queries: Dict[str, Optional[str]],
    task_count: Optional[int] = None,
    pod_count: Optional[int] = None,
) -> Tuple[str, bool]:
    used_queries = [query for query in energy_queries.values() if query]
    used_fallback = any("avg_over_time" in query for query in used_queries)
    metric_name = detected_energy_metric or "Kepler-derived energy metric"
    scope = (
        f" across {pod_count} pod(s) ({task_count} task(s))"
        if pod_count is not None and task_count is not None
        else ""
    )

    if used_queries and not used_fallback:
        method = (
            f"Energy collected from Prometheus using Kepler metric '{metric_name}' "
            f"as the sum of per-pod counter increases{scope}."
        )
    elif used_queries and used_fallback:
        method = (
            f"Energy collected from Prometheus using Kepler metric '{metric_name}'{scope}. "
            "Per-pod counter increases were used where available; "
            "avg_over_time was used as a fallback for pods with insufficient scrape points."
        )
    else:
        method = f"Energy could not be derived from Prometheus/Kepler metrics{scope}."

    return method, used_fallback


def summarize_carbon_method(carbon_intensity: float, has_energy_value: bool) -> str:
    if has_energy_value:
        return (
            "Carbon emissions were calculated using the emissions factor method: "
            "energy consumption (kWh) multiplied by the carbon intensity factor "
            f"({carbon_intensity} kg CO2e/kWh)."
        )
    return (
        "Carbon emissions could not be derived because workflow energy consumption could not be "
        "derived from Prometheus/Kepler metrics for this run."
    )


def summarize_cpu_method(task_count: int, pod_count: int, max_over_time_count: int) -> str:
    increase_count = pod_count - max_over_time_count
    if max_over_time_count == 0:
        query_desc = "increase() over the workflow window"
    elif increase_count == 0:
        query_desc = "max_over_time() fallback for all pods"
    else:
        query_desc = (
            f"increase() for {increase_count} pod(s); "
            f"max_over_time() fallback for {max_over_time_count} pod(s) "
            "where increase() returned zero"
        )
    return (
        f"Sum of Prometheus container_cpu_usage_seconds_total across {pod_count} pod(s) "
        f"({task_count} task(s)) using {query_desc}."
    )


def summarize_duration_method(task_count: int) -> str:
    return (
        f"Wall-clock elapsed time from the earliest task start to the latest task end "
        f"across {task_count} task(s) in this group."
    )


def summarize_overall_duration_method(task_count: int) -> str:
    return (
        f"Wall-clock elapsed time from the earliest task start to the latest task end "
        f"across all {task_count} task(s) in this workflow run."
    )


def summarize_overall_cpu_method(task_count: int, pod_count: int, max_over_time_count: int) -> str:
    increase_count = pod_count - max_over_time_count
    if max_over_time_count == 0:
        query_desc = "increase()"
    elif increase_count == 0:
        query_desc = "max_over_time() fallback for all pods"
    else:
        query_desc = (
            f"increase() for {increase_count} pod(s) and "
            f"max_over_time() fallback for {max_over_time_count} pod(s) "
            "where increase() returned zero"
        )
    return (
        f"Sum of Prometheus container_cpu_usage_seconds_total across all workflow task pods "
        f"({pod_count} pod(s) / {task_count} task(s)) over the workflow window using {query_desc}. "
        "Because tasks run in parallel, total CPU time can exceed wall-clock duration."
    )


def summarize_parallelism_note(task_count: int, pod_count: int) -> str:
    return (
        f"Tasks in this group run in parallel, so CPU time is the total work done across "
        f"all {pod_count} pod(s) while duration is the compressed wall-clock window. "
        "CPU time will exceed duration whenever more than one task runs simultaneously."
    )


def classify_gpu_usage_status(
    gpu_requested: bool,
    gpu_capable_node_used: bool,
    gpu_metric_available: bool,
) -> str:
    if gpu_requested and gpu_metric_available:
        return "GPU-enabled workflow run"
    if gpu_requested:
        return "GPU-requested workflow run"
    if gpu_capable_node_used:
        return "CPU-only workflow run on GPU-capable hardware"
    return "CPU-only workflow run"


def _tz_label(dt: datetime) -> str:
    """Return 'CEST' during European summer time, 'CET' otherwise."""
    import zoneinfo
    berlin = zoneinfo.ZoneInfo("Europe/Berlin")
    aware = dt.replace(tzinfo=berlin) if dt.tzinfo is None else dt.astimezone(berlin)
    return "CEST" if aware.dst() and aware.dst().total_seconds() > 0 else "CET"


def format_run_title_label(start_local: datetime, end_local: datetime) -> str:
    tz = _tz_label(start_local)
    if start_local.date() == end_local.date():
        return (
            f"Execution metadata · {start_local.strftime('%d %b %Y')}, "
            f"{start_local.strftime('%H:%M')}–{end_local.strftime('%H:%M')} {tz}"
        )
    return (
        f"Execution metadata · {start_local.strftime('%d %b %Y')} {start_local.strftime('%H:%M')} "
        f"– {end_local.strftime('%d %b %Y')} {end_local.strftime('%H:%M')} {tz}"
    )


ELECTRICITYMAP_API_PAST_RANGE = "https://api.electricitymap.org/v3/carbon-intensity/past-range"
ELECTRICITYMAP_API_LATEST    = "https://api.electricitymap.org/v3/carbon-intensity/latest"


def _em_request(url: str, token: str) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"auth-token": token})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_carbon_intensity_kg(
    zone: str,
    token: str,
    start_utc: datetime,
    end_utc: datetime,
) -> Tuple[Optional[float], str]:
    """
    Fetch carbon intensity (kg CO₂e/kWh) from ElectricityMaps.

    Tries /past-range first (commercial plan) to get the average over the
    actual execution window.  Falls back to /latest (free tier) if the
    history endpoint returns a 4xx (plan restriction).

    Returns (intensity_kg_per_kwh, source_label) where source_label is one of:
      "electricitymap-history-average"  – past-range succeeded
      "electricitymap-latest"           – fell back to latest value
      "electricitymap-error:<msg>"      – API reachable but bad data
      "electricitymap-unavailable:<msg>"– network / auth failure
    """
    start_str = start_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_str   = end_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    history_url = (
        f"{ELECTRICITYMAP_API_PAST_RANGE}?"
        f"{urlencode({'zone': zone, 'start': start_str, 'end': end_str})}"
    )
    latest_url = f"{ELECTRICITYMAP_API_LATEST}?{urlencode({'zone': zone})}"

    # --- attempt 1: past-range (average over execution window) ---
    try:
        data = _em_request(history_url, token)
        points = data.get("data", [])
        intensities = []
        for point in points:
            val = point.get("carbonIntensity")
            if val is not None:
                try:
                    intensities.append(float(val))
                except (TypeError, ValueError):
                    pass
        if intensities:
            avg_kg = sum(intensities) / len(intensities) / 1000.0
            return avg_kg, "electricitymap-history-average"
        # got a response but no usable data – fall through to latest
    except urllib.error.HTTPError as exc:
        if exc.code not in (401, 403, 404):
            return None, f"electricitymap-unavailable:HTTP {exc.code}"
        # 401/403/404 → endpoint not on this plan, try latest
    except Exception as exc:
        return None, f"electricitymap-unavailable:{exc}"

    # --- attempt 2: latest (free tier) ---
    try:
        data = _em_request(latest_url, token)
        val = data.get("carbonIntensity")
        if val is not None:
            return float(val) / 1000.0, "electricitymap-latest"
        return None, "electricitymap-error:no-carbonIntensity-in-latest"
    except Exception as exc:
        return None, f"electricitymap-unavailable:{exc}"


def prom_query_range(prom_url: str, query: str, start: datetime, end: datetime, step_seconds: int) -> Any:
    params = {
        "query": query,
        "start": str(start.timestamp()),
        "end": str(end.timestamp()),
        "step": f"{step_seconds}s",
    }
    data = http_get_json(f"{prom_url}/api/v1/query_range", params)
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus range query failed:\n{query}\n{data}")
    return data["data"]["result"]


def first_value(result: Any) -> Optional[float]:
    if not result:
        return None
    try:
        return float(result[0]["value"][1])
    except Exception:
        return None


def airflow_exec(airflow_namespace: str, scheduler_pod: str, args: Sequence[str]) -> str:
    return run_cmd(["kubectl", "-n", airflow_namespace, "exec", scheduler_pod, "--", *args])


def airflow_exec_sh(airflow_namespace: str, scheduler_pod: str, script: str) -> str:
    return run_cmd(["kubectl", "-n", airflow_namespace, "exec", scheduler_pod, "--", "sh", "-lc", script])


def detect_scheduler_pod(airflow_namespace: str) -> str:
    data = kubectl_json(["get", "pod", "-n", airflow_namespace, "-l", "component=scheduler"])
    items = data.get("items", [])
    if not items:
        raise RuntimeError(f"No scheduler pod found in namespace '{airflow_namespace}'")
    items.sort(key=lambda item: item["metadata"]["creationTimestamp"])
    return items[-1]["metadata"]["name"]


def parse_airflow_table(output: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    headers: Optional[List[str]] = None

    for raw_line in output.splitlines():
        line = raw_line.rstrip()
        if "|" not in line:
            continue
        parts = [part.strip() for part in line.split("|")]
        if not parts:
            continue
        if headers is None:
            headers = parts
            continue
        if all(set(part) <= {"=", "+"} for part in parts):
            continue
        if len(parts) != len(headers):
            continue
        rows.append(dict(zip(headers, parts)))
    return rows


def parse_nullable_datetime(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    value = value.strip()
    if not value or value == "None":
        return None
    return parse_ts(value)


def select_run(rows: List[Dict[str, str]], run_id: Optional[str], execution_date: Optional[str]) -> Dict[str, str]:
    if run_id:
        for row in rows:
            if row.get("run_id") == run_id:
                return row
        raise RuntimeError(f"Run id '{run_id}' not found")
    if execution_date:
        for row in rows:
            if row.get("execution_date") == execution_date:
                return row
        raise RuntimeError(f"Execution date '{execution_date}' not found")
    if not rows:
        raise RuntimeError("No DAG runs found")
    return rows[0]


def get_run_record(airflow_namespace: str, scheduler_pod: str, dag_id: str, run_id: Optional[str], execution_date: Optional[str]) -> Dict[str, Any]:
    output = airflow_exec(airflow_namespace, scheduler_pod, ["airflow", "dags", "list-runs", "-d", dag_id])
    rows = parse_airflow_table(output)
    row = select_run(rows, run_id, execution_date)
    return {
        "dag_id": row["dag_id"],
        "run_id": row["run_id"],
        "state": row["state"],
        "execution_date": row["execution_date"],
        "start_date": parse_nullable_datetime(row.get("start_date")),
        "end_date": parse_nullable_datetime(row.get("end_date")),
    }


def get_task_records(airflow_namespace: str, scheduler_pod: str, dag_id: str, execution_date: str) -> List[Dict[str, Any]]:
    output = airflow_exec(
        airflow_namespace,
        scheduler_pod,
        ["airflow", "tasks", "states-for-dag-run", dag_id, execution_date],
    )
    rows = parse_airflow_table(output)
    tasks: List[Dict[str, Any]] = []
    for row in rows:
        state = row.get("state")
        tasks.append(
            {
                "task_id": row["task_id"],
                "state": None if state == "None" else state,
                "start_date": parse_nullable_datetime(row.get("start_date")),
                "end_date": parse_nullable_datetime(row.get("end_date")),
            }
        )
    return tasks


def task_log_dir(dag_id: str, run_id: str, task_id: str) -> str:
    return f"/opt/airflow/logs/dag_id={dag_id}/run_id={run_id}/task_id={task_id}"


def read_task_logs(airflow_namespace: str, scheduler_pod: str, dag_id: str, run_id: str, task_id: str) -> str:
    log_dir = task_log_dir(dag_id, run_id, task_id)
    script = (
        f"dir={shlex.quote(log_dir)}; "
        'if [ -d "$dir" ]; then '
        'for f in "$dir"/attempt=*.log; do '
        '[ -f "$f" ] || continue; '
        'cat "$f"; printf "\\n"; '
        'done; '
        "fi"
    )
    return airflow_exec_sh(airflow_namespace, scheduler_pod, script)


def extract_pod_names(log_text: str) -> List[str]:
    seen: List[str] = []
    for line in log_text.splitlines():
        for pattern in POD_NAME_PATTERNS:
            match = pattern.search(line)
            if match:
                pod_name = match.group(1)
                if pod_name not in seen:
                    seen.append(pod_name)
    return seen


def build_pod_records(
    airflow_namespace: str,
    scheduler_pod: str,
    dag_id: str,
    run_id: str,
    tasks: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    pods: List[Dict[str, Any]] = []
    for task in tasks:
        if task["start_date"] is None and task["end_date"] is None and task["state"] in (None, "scheduled", "queued"):
            continue
        if task["task_id"] == "Start":
            continue
        log_text = read_task_logs(airflow_namespace, scheduler_pod, dag_id, run_id, task["task_id"])
        pod_names = extract_pod_names(log_text)
        if not pod_names:
            continue
        for pod_name in pod_names:
            pods.append(
                {
                    "task_id": task["task_id"],
                    "state": task["state"],
                    "start_date": task["start_date"],
                    "end_date": task["end_date"],
                    "pod_name": pod_name,
                }
            )
    return pods


def try_instant_queries(prom_url: str, queries: Iterable[str]) -> Tuple[Optional[float], Optional[str]]:
    for query in queries:
        try:
            result = prom_query(prom_url, query)
            value = first_value(result)
            if value is not None:
                return value, query
        except Exception:
            continue
    return None, None


def build_cpu_queries(namespace: str, pod_name: str, window: str, at_expr: str) -> List[str]:
    return [
        f'sum(increase(container_cpu_usage_seconds_total{{namespace="{namespace}",pod="{pod_name}",container!="",image!=""}}{window}{at_expr}))',
        f'sum(increase(container_cpu_usage_seconds_total{{namespace="{namespace}",pod="{pod_name}",container!="POD"}}{window}{at_expr}))',
        f'sum(increase(container_cpu_usage_seconds_total{{pod="{pod_name}",container!="",image!=""}}{window}{at_expr}))',
    ]


def _cpu_selectors(namespace: str, pod_name: str) -> List[str]:
    return [
        f'container_cpu_usage_seconds_total{{namespace="{namespace}",pod="{pod_name}",container!="",image!=""}}',
        f'container_cpu_usage_seconds_total{{namespace="{namespace}",pod="{pod_name}",container!="POD"}}',
        f'container_cpu_usage_seconds_total{{pod="{pod_name}",container!="",image!=""}}',
    ]


def query_cpu_for_pod(
    prom_url: str,
    namespace: str,
    pod_name: str,
    windows_and_anchors: List[Tuple[str, str]],
) -> Tuple[Optional[float], Optional[str]]:
    """Query total CPU seconds for a pod across multiple window/anchor combinations.

    Tries increase() first (accurate when ≥2 scrape points exist).  Falls back
    to max_over_time() which works even for single-scrape-point pods because
    Kubernetes resets each container's counter to 0 at start, so the highest
    observed value equals total CPU consumed.
    """
    selectors = _cpu_selectors(namespace, pod_name)
    zero_fallback: Tuple[Optional[float], Optional[str]] = (None, None)

    # Pass 1: increase() — preferred; skip 0.0 so we can try max_over_time next.
    for window, at_expr in windows_and_anchors:
        for sel in selectors:
            query = f"sum(increase({sel}{window}{at_expr}))"
            try:
                value = first_value(prom_query(prom_url, query))
                if value is not None:
                    if value > 0:
                        return value, query
                    if zero_fallback[0] is None:
                        zero_fallback = (value, query)
            except Exception:
                continue

    # Pass 2: max_over_time — reliable for short-lived pods (single scrape point).
    # Counter starts at 0 per pod, so max == total CPU used.
    for window, at_expr in windows_and_anchors:
        for sel in selectors:
            query = f"sum(max_over_time({sel}{window}{at_expr}))"
            try:
                value = first_value(prom_query(prom_url, query))
                if value is not None and value > 0:
                    return value, query
            except Exception:
                continue

    return zero_fallback


def build_energy_queries(namespace: str, pod_name: str, window: str, at_expr: str) -> List[str]:
    return [
        f'sum(increase(kepler_container_joules_total{{namespace="{namespace}",pod_name="{pod_name}"}}{window}{at_expr}))',
        f'sum(increase(kepler_container_joules_total{{namespace="{namespace}",pod="{pod_name}"}}{window}{at_expr}))',
        f'sum(increase(kepler_container_package_joules_total{{namespace="{namespace}",pod_name="{pod_name}"}}{window}{at_expr}))',
        f'sum(increase(kepler_container_package_joules_total{{namespace="{namespace}",pod="{pod_name}"}}{window}{at_expr}))',
    ]


def query_energy_for_pod(
    prom_url: str,
    metric: str,
    namespace: str,
    pod_name: str,
    window: str,
    duration_s: int,
    at_expr: str,
) -> Tuple[Optional[float], Optional[str]]:
    selectors = [
        f'{metric}{{namespace="{namespace}",pod="{pod_name}"}}',
        f'{metric}{{namespace="{namespace}",pod_name="{pod_name}"}}',
        f'{metric}{{pod="{pod_name}"}}',
        f'{metric}{{pod_name="{pod_name}"}}',
    ]
    queries: List[str] = []
    for selector in selectors:
        queries.append(f"sum(increase({selector}{window}{at_expr}))")
        queries.append(f"sum(avg_over_time({selector}{window}{at_expr})) * {duration_s}")
    return try_instant_queries(prom_url, queries)


def build_memory_range_queries(namespace: str, pod_name: str) -> List[str]:
    return [
        f'sum(container_memory_working_set_bytes{{namespace="{namespace}",pod="{pod_name}",container!="",image!=""}})',
        f'sum(container_memory_working_set_bytes{{namespace="{namespace}",pod="{pod_name}",container!="POD"}})',
        f'sum(container_memory_working_set_bytes{{pod="{pod_name}",container!="",image!=""}})',
    ]


def query_memory_series(
    prom_url: str,
    namespace: str,
    pod_name: str,
    start: datetime,
    end: datetime,
    step_seconds: int,
) -> Tuple[Dict[int, float], Optional[str]]:
    for query in build_memory_range_queries(namespace, pod_name):
        try:
            result = prom_query_range(prom_url, query, start, end, step_seconds)
        except Exception:
            continue
        if not result:
            continue
        values = result[0].get("values", [])
        if not values:
            continue
        series = {int(float(ts)): float(value) for ts, value in values}
        return series, query
    return {}, None


def build_pod_info_queries(namespace: str, pod_name: str, ts: datetime) -> List[str]:
    at_expr = prom_at(ts)
    return [
        f'kube_pod_info{{namespace="{namespace}",pod="{pod_name}"}}{at_expr}',
        f'kube_pod_info{{pod="{pod_name}"}}{at_expr}',
    ]


def build_pod_container_info_queries(namespace: str, pod_name: str, ts: datetime) -> List[str]:
    at_expr = prom_at(ts)
    return [
        f'kube_pod_container_info{{namespace="{namespace}",pod="{pod_name}"}}{at_expr}',
        f'kube_pod_container_info{{pod="{pod_name}"}}{at_expr}',
    ]


def candidate_probe_times(start: Optional[datetime], end: Optional[datetime], workflow_start: datetime, workflow_end: datetime) -> List[datetime]:
    times: List[datetime] = []
    if start:
        times.append(start + timedelta(seconds=5))
        times.append(start + timedelta(seconds=30))
    if start and end and end > start:
        times.append(start + (end - start) / 2)
        times.append(max(start, end - timedelta(seconds=5)))
    if end:
        times.append(end)
    times.append(workflow_start + timedelta(seconds=5))
    times.append(workflow_end)

    uniq: List[datetime] = []
    for ts in times:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts < workflow_start:
            ts = workflow_start
        if ts > workflow_end:
            ts = workflow_end
        if ts not in uniq:
            uniq.append(ts)
    return uniq


def query_pod_info(prom_url: str, namespace: str, pod_name: str, start: Optional[datetime], end: Optional[datetime], workflow_start: datetime, workflow_end: datetime) -> Dict[str, Optional[str]]:
    node_name: Optional[str] = None
    image_names: List[str] = []

    for ts in candidate_probe_times(start, end, workflow_start, workflow_end):
        for query in build_pod_info_queries(namespace, pod_name, ts):
            try:
                result = prom_query(prom_url, query)
            except Exception:
                continue
            if result:
                node_name = result[0].get("metric", {}).get("node") or node_name
                break
        if node_name:
            break

    for ts in candidate_probe_times(start, end, workflow_start, workflow_end):
        for query in build_pod_container_info_queries(namespace, pod_name, ts):
            try:
                result = prom_query(prom_url, query)
            except Exception:
                continue
            for row in result:
                image = row.get("metric", {}).get("image")
                if image and image not in image_names:
                    image_names.append(image)
            if image_names:
                break
        if image_names:
            break

    return {
        "node_name": node_name,
        "images": image_names,
    }


def parse_images_from_code(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()
    images: List[str] = []
    for match in IMAGE_PATTERN.finditer(text):
        image = match.group(1)
        if image not in images:
            images.append(image)
    return images


def detect_gpu_requests_from_code(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()
    gpu_keys: List[str] = []
    for match in GPU_RESOURCE_REGEX.finditer(text):
        key = match.group(1)
        if key not in gpu_keys:
            gpu_keys.append(key)
    return gpu_keys


def flatten_unique(values: Iterable[Optional[str]]) -> List[str]:
    result: List[str] = []
    for value in values:
        if not value:
            continue
        if value not in result:
            result.append(value)
    return result


def classify_workflow_process(task_id: str) -> Optional[Tuple[str, str]]:
    if task_id.startswith("preprocess_level2_"):
        return ("level2_preprocessing", "Level-2 preprocessing")
    if task_id.startswith("tsa_task_"):
        return ("time_series_analysis", "Time-series analysis")
    if task_id.startswith("pyramid_task_"):
        return ("pyramid_generation", "Pyramid generation")
    if task_id.startswith("mosaic_task_"):
        return ("mosaic_generation", "Mosaic generation")
    if task_id in OVERHEAD_TASK_IDS or task_id.startswith("generate_") or task_id.startswith("prepare_") or task_id.startswith("wait_"):
        return ("workflow_overhead", "Workflow overhead")
    return None


def build_process_groups(tasks: List[Dict[str, Any]], pod_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[str, Dict[str, Any]] = {}

    for task in tasks:
        classified = classify_workflow_process(task["task_id"])
        if not classified:
            continue
        slug, label = classified
        group = groups.setdefault(slug, {"slug": slug, "label": label, "tasks": [], "pods": []})
        group["tasks"].append(task)

    for pod in pod_records:
        classified = classify_workflow_process(pod["task_id"])
        if not classified:
            continue
        slug, label = classified
        group = groups.setdefault(slug, {"slug": slug, "label": label, "tasks": [], "pods": []})
        group["pods"].append(pod)

    order = [
        "level2_preprocessing",
        "time_series_analysis",
        "pyramid_generation",
        "mosaic_generation",
        "workflow_overhead",
    ]
    return [groups[key] for key in order if key in groups]


def aggregate_memory(
    prom_url: str,
    namespace: str,
    pod_names: List[str],
    start: datetime,
    end: datetime,
    step_seconds: int,
) -> Dict[str, Any]:
    totals: Dict[int, float] = defaultdict(float)
    queries: Dict[str, str] = {}

    for pod_name in pod_names:
        series, query = query_memory_series(prom_url, namespace, pod_name, start, end, step_seconds)
        if query:
            queries[pod_name] = query
        for ts, value in series.items():
            totals[ts] += value

    if not totals:
        return {"avg": None, "peak": None, "queries": queries}

    ordered_values = [totals[key] for key in sorted(totals)]
    avg = sum(ordered_values) / float(len(ordered_values))
    peak = max(ordered_values)
    return {"avg": avg, "peak": peak, "queries": queries}


def stringify_unique(values: Iterable[str]) -> List[str]:
    result: List[str] = []
    for value in values:
        if value not in result:
            result.append(value)
    return result


def extract_gpu_fields(mapping: Optional[Dict[str, Any]]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for key, value in (mapping or {}).items():
        low = key.lower()
        if "gpu" in low or "nvidia" in low or "cuda" in low:
            result[str(key)] = str(value)
    return result


def node_is_gpu_capable(node_record: Dict[str, Any]) -> bool:
    labels = node_record.get("labels", {})
    allocatable_gpu = extract_gpu_fields(node_record.get("allocatable", {}))
    return (
        labels.get("nvidia.com/gpu.present", "").lower() == "true"
        or any("gpu.product" in key.lower() for key in labels)
        or bool(allocatable_gpu)
    )


def summarize_node_gpu(node_record: Dict[str, Any]) -> Optional[str]:
    labels = node_record.get("labels", {})
    allocatable_gpu = extract_gpu_fields(node_record.get("allocatable", {}))
    parts: List[str] = [node_record["name"]]
    product = labels.get("nvidia.com/gpu.product")
    if product:
        parts.append(product)
    gpu_count = labels.get("nvidia.com/gpu.count")
    if gpu_count:
        parts.append(f"count={gpu_count}")
    if allocatable_gpu:
        alloc_str = ", ".join(f"{key}={value}" for key, value in sorted(allocatable_gpu.items()))
        parts.append(f"allocatable={alloc_str}")
    if len(parts) == 1:
        return None
    return " | ".join(parts)


def build_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect VIVO-friendly metadata for one Airflow workflow run aggregated across all task pods."
    )
    parser.add_argument("--airflow-namespace", default="airflow-yagmur")
    parser.add_argument("--scheduler-pod", default=None)
    parser.add_argument("--namespace", default="default", help="Namespace where the workflow task pods run")
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--execution-date", default=None)
    parser.add_argument(
        "--workflow-name",
        default=None,
        help="Public-facing workflow name to use in the metadata output instead of the internal Airflow dag_id",
    )
    parser.add_argument(
        "--workflow-engine",
        default="Apache Airflow",
        help="Workflow engine name to record in the metadata output",
    )
    parser.add_argument("--code-name", required=True)
    parser.add_argument("--code-path", required=True)
    parser.add_argument(
        "--code-uri",
        default=None,
        help="Optional public URI for the code or DAG definition, for example a GitHub blob URL",
    )
    parser.add_argument(
        "--trace-archive",
        default=None,
        help="Optional URI pointing to a trace/log archive for this workflow run",
    )
    parser.add_argument(
        "--trace-types",
        default=None,
        help="Optional free-text description of the kinds of traces in the archive (e.g. 'Airflow Logs, Force logs')",
    )
    parser.add_argument(
        "--trace-data-format",
        default=None,
        help="Optional free-text description of the trace data format (e.g. 'Zipped text files')",
    )
    parser.add_argument(
        "--application-domain-uri",
        default=None,
        help="Optional URI of an Application domain individual to link this workflow to (e.g. the Remote sensing individual)",
    )
    parser.add_argument(
        "--responsible-researcher-uri",
        default=None,
        help="Optional URI of the responsible researcher to link this workflow to",
    )
    parser.add_argument(
        "--cluster-label",
        default="Fonda Cluster",
        help="Human-readable label for the compute cluster entity",
    )
    parser.add_argument(
        "--cluster-slug",
        default="fonda-cluster",
        help="Slug used in the compute cluster URI (e.g. fonda-cluster)",
    )
    parser.add_argument(
        "--workflow-ui-base-url",
        default="http://127.0.0.1:8080",
        help="Base URL used to construct a workflow UI link, for example a local Airflow port-forward URL",
    )
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--prom-url", default=PROM_URL_DEFAULT)
    parser.add_argument("--base-uri", default=BASE_URI_DEFAULT)
    parser.add_argument("--ontology-uri", default=ONTOLOGY_URI_DEFAULT)
    parser.add_argument("--software-uri", default=None)
    parser.add_argument(
        "--publication-uri",
        default=None,
        help="Existing VIVO publication URI to link the workflow run to, using the same pattern as the publication-oriented metadata script",
    )
    parser.add_argument("--software-title", default=None)
    parser.add_argument("--carbon-intensity", type=float, default=CARBON_INTENSITY_DEFAULT)
    parser.add_argument(
        "--electricitymap-token",
        default=os.environ.get("ELECTRICITYMAP_TOKEN"),
        help="ElectricityMaps API token (overrides ELECTRICITYMAP_TOKEN env var). "
             "When set, the actual carbon intensity over the workflow execution window "
             "is fetched from the ElectricityMaps history API and used instead of "
             "--carbon-intensity.",
    )
    parser.add_argument(
        "--electricitymap-zone",
        default="DE",
        help="ElectricityMaps zone code (default: DE)",
    )
    parser.add_argument("--memory-step-seconds", type=int, default=30)
    parser.add_argument(
        "--exclude-task-ids",
        default="",
        help="Comma-separated task ids to exclude from workflow status/metrics aggregation",
    )
    parser.add_argument(
        "--language",
        default=None,
        help="Programming language of the workflow (e.g. Python, R, Snakemake)",
    )
    return parser.parse_args()


def filter_tasks(tasks: List[Dict[str, Any]], excluded_task_ids: Sequence[str]) -> List[Dict[str, Any]]:
    excluded = set(excluded_task_ids)
    return [task for task in tasks if task["task_id"] not in excluded]


def derive_run_state(tasks: List[Dict[str, Any]], fallback_state: str) -> str:
    if not tasks:
        return fallback_state
    states = [task["state"] for task in tasks]
    failed_states = {"failed", "upstream_failed", "shutdown", "removed"}
    active_states = {"running", "queued", "scheduled", "up_for_retry", "up_for_reschedule", "restarting", "deferred"}
    success_like_states = {"success", "skipped", None}

    if any(state in failed_states for state in states):
        return "failed"
    if any(state in active_states for state in states):
        return "running"
    if all(state in success_like_states for state in states):
        if any(state is None for state in states):
            return fallback_state
        return "success"
    return fallback_state


def main() -> None:
    args = build_args()
    ensure_prometheus_reachable(args.prom_url)
    metric_names = prom_metric_names(args.prom_url)
    detected_energy_metric = find_energy_metric(metric_names)
    excluded_task_ids = split_csv(args.exclude_task_ids)
    linked_resource_uri = args.publication_uri or args.software_uri

    if args.publication_uri and args.software_uri:
        raise RuntimeError("Use either --publication-uri or --software-uri, not both")

    if args.software_uri and looks_like_publication_uri(args.software_uri):
        print(
            "WARNING: --software-uri looks like a publication URI. "
            "Consider using --publication-uri instead for workflow-to-article linking.",
            file=sys.stderr,
        )

    scheduler_pod = args.scheduler_pod or detect_scheduler_pod(args.airflow_namespace)
    run_record = get_run_record(args.airflow_namespace, scheduler_pod, args.dag_id, args.run_id, args.execution_date)
    all_tasks = get_task_records(args.airflow_namespace, scheduler_pod, args.dag_id, run_record["execution_date"])
    tasks = filter_tasks(all_tasks, excluded_task_ids)

    run_start = run_record["start_date"]
    run_end = run_record["end_date"] or datetime.now(timezone.utc)
    if run_start is None:
        task_starts = [task["start_date"] for task in tasks if task["start_date"] is not None]
        if not task_starts:
            raise RuntimeError("Could not determine workflow start time")
        run_start = min(task_starts)
    else:
        included_starts = [task["start_date"] for task in tasks if task["start_date"] is not None]
        if included_starts:
            run_start = min(included_starts)

    included_ends = [task["end_date"] for task in tasks if task["end_date"] is not None]
    if included_ends and not any(task["state"] in {"running", "queued", "scheduled"} for task in tasks):
        run_end = max(included_ends)

    # --- Live carbon intensity (ElectricityMaps) ---
    carbon_intensity_source = "fallback-default"
    if args.electricitymap_token:
        live_intensity, carbon_intensity_source = fetch_carbon_intensity_kg(
            zone=args.electricitymap_zone,
            token=args.electricitymap_token,
            start_utc=run_start,
            end_utc=run_end,
        )
        if live_intensity is not None:
            args.carbon_intensity = live_intensity
            print(
                f"carbon_intensity={live_intensity:.6f} kg CO2e/kWh "
                f"(live average for zone {args.electricitymap_zone})",
                file=sys.stderr,
            )
        else:
            print(
                f"WARNING: ElectricityMaps fetch failed ({carbon_intensity_source}), "
                f"falling back to --carbon-intensity={args.carbon_intensity}",
                file=sys.stderr,
            )

    pod_records = build_pod_records(args.airflow_namespace, scheduler_pod, args.dag_id, run_record["run_id"], tasks)
    pod_names = stringify_unique(record["pod_name"] for record in pod_records)
    if not pod_names:
        raise RuntimeError("Could not find any workflow task pods from Airflow logs")

    workflow_window = build_window(run_start, run_end)
    at_expr = prom_at(run_end)

    total_cpu = 0.0
    total_energy_joules = 0.0
    cpu_queries: Dict[str, Optional[str]] = {}
    energy_queries: Dict[str, Optional[str]] = {}

    node_names: List[str] = []
    images: List[str] = []

    for pod in pod_records:
        pod_name = pod["pod_name"]
        cpu_value, cpu_query = query_cpu_for_pod(
            args.prom_url, args.namespace, pod_name, [(workflow_window, at_expr)]
        )
        energy_value, energy_query = try_instant_queries(args.prom_url, build_energy_queries(args.namespace, pod_name, workflow_window, at_expr))
        if energy_value is None and detected_energy_metric:
            energy_value, energy_query = query_energy_for_pod(
                args.prom_url,
                detected_energy_metric,
                args.namespace,
                pod_name,
                workflow_window,
                duration_s=seconds_between(run_start, run_end),
                at_expr=at_expr,
            )
        if cpu_value is not None:
            total_cpu += cpu_value
        if energy_value is not None:
            total_energy_joules += energy_value
        cpu_queries[pod_name] = cpu_query
        energy_queries[pod_name] = energy_query

        pod_info = query_pod_info(
            args.prom_url,
            args.namespace,
            pod_name,
            pod["start_date"],
            pod["end_date"],
            run_start,
            run_end,
        )
        node_names.extend(pod_info["node_name"] and [pod_info["node_name"]] or [])
        images.extend(pod_info["images"])

    memory = aggregate_memory(args.prom_url, args.namespace, pod_names, run_start, run_end, args.memory_step_seconds)

    if not any(cpu_queries.values()) and memory["avg"] is None and memory["peak"] is None and not any(energy_queries.values()):
        raise RuntimeError(
            "No Prometheus metrics were found for this workflow run. "
            "Check that Prometheus/Kepler are scraped for these task pods and that the port-forward is still active."
        )

    unique_nodes = stringify_unique(node_names)
    unique_images = stringify_unique(images)
    if not unique_images:
        unique_images = parse_images_from_code(args.code_path)

    node_infos: List[Dict[str, Any]] = []
    for node_name in unique_nodes:
        node = kubectl_json(["get", "node", node_name])
        node_infos.append(
            {
                "name": node_name,
                "node_info": node.get("status", {}).get("nodeInfo", {}),
                "allocatable": node.get("status", {}).get("allocatable", {}),
                "labels": node.get("metadata", {}).get("labels", {}),
            }
        )

    allocatable_cpus = stringify_unique(str(info["allocatable"].get("cpu")) for info in node_infos if info.get("allocatable"))
    allocatable_mems = stringify_unique(str(info["allocatable"].get("memory")) for info in node_infos if info.get("allocatable"))
    allocatable_mem_gbs = [
        bytes_to_gb(parse_k8s_bytes(value))
        for value in allocatable_mems
    ]
    allocatable_mem_gbs = [value for value in allocatable_mem_gbs if value is not None]
    architectures = stringify_unique(str(info["node_info"].get("architecture")) for info in node_infos if info.get("node_info"))
    kernel_versions = stringify_unique(str(info["node_info"].get("kernelVersion")) for info in node_infos if info.get("node_info"))
    kubelet_versions = stringify_unique(str(info["node_info"].get("kubeletVersion")) for info in node_infos if info.get("node_info"))
    os_images = stringify_unique(str(info["node_info"].get("osImage")) for info in node_infos if info.get("node_info"))
    backend = detect_backend(
        pod_names,
        node_infos,
        [args.workflow_engine, args.airflow_namespace, args.namespace, args.code_path],
    )
    execution_hosts = unique_nodes[:]
    gpu_metric_available = has_gpu_metric_support(metric_names)
    gpu_request_keys = detect_gpu_requests_from_code(args.code_path)
    gpu_requested = bool(gpu_request_keys)
    gpu_capable_nodes_used = [info for info in node_infos if node_is_gpu_capable(info)]
    gpu_capable_node_used = bool(gpu_capable_nodes_used)
    gpu_node_summaries = stringify_unique(
        summary
        for summary in (summarize_node_gpu(info) for info in gpu_capable_nodes_used)
        if summary
    )

    duration_s = seconds_between(run_start, run_end)
    energy_kwh = joules_to_kwh(total_energy_joules) if total_energy_joules else None
    carbon_kg = energy_kwh * args.carbon_intensity if energy_kwh is not None else None
    memory_avg_gb = bytes_to_gb(memory["avg"])
    memory_peak_gb = bytes_to_gb(memory["peak"])
    energy_method, energy_method_uses_fallback = summarize_energy_method(detected_energy_metric, energy_queries)
    carbon_method = summarize_carbon_method(args.carbon_intensity, energy_kwh is not None)
    code_version = f"sha256:{sha256_file(args.code_path)}"
    effective_state = derive_run_state(tasks, run_record["state"])
    workflow_name = args.workflow_name or args.code_name or args.dag_id
    public_workflow_name = args.code_name
    if args.workflow_name and args.code_name.strip() == args.dag_id.strip():
        public_workflow_name = args.workflow_name
    workflow_ui_link = build_workflow_ui_link(args.workflow_ui_base_url, args.dag_id)
    metadata_generated_at = datetime.now(BERLIN_TZ)
    run_start_local = run_start.astimezone(BERLIN_TZ)
    run_end_local = run_end.astimezone(BERLIN_TZ)
    run_start_local_naive = naive_local_iso(run_start_local)
    run_end_local_naive = naive_local_iso(run_end_local)
    process_groups = build_process_groups(tasks, pod_records)

    base_uri = args.base_uri.rstrip("/") + "/"
    ontology_uri = args.ontology_uri if args.ontology_uri.endswith(("#", "/")) else args.ontology_uri + "#"
    run_slug = slugify(f"{args.namespace}-{workflow_name}-{run_record['run_id']}-{run_start.isoformat()}")
    run_uri = f"<{base_uri}run/{run_slug}>"
    software_uri = as_ttl_uri(linked_resource_uri) if linked_resource_uri else f"<{base_uri}software/{slugify(workflow_name)}>"
    metadata_title = format_run_title_label(run_start_local, run_end_local)
    workflow_entity_uri = f"<{base_uri}workflow/{slugify(public_workflow_name or workflow_name)}>"
    cluster_uri = f"<{base_uri}cluster/{args.cluster_slug}>"
    engine_uri = f"<{base_uri}engine/{slugify(args.workflow_engine)}>"
    language_uri = f"<{base_uri}language/{slugify(args.language)}>" if args.language else None
    total_max_over_time_count = sum(1 for q in cpu_queries.values() if q and "max_over_time" in q)
    overall_cpu_method = summarize_overall_cpu_method(len(tasks), len(pod_names), total_max_over_time_count)
    overall_duration_method = summarize_overall_duration_method(len(tasks))
    process_records: List[Dict[str, Any]] = []

    gpu_usage_status = classify_gpu_usage_status(
        gpu_requested=gpu_requested,
        gpu_capable_node_used=gpu_capable_node_used,
        gpu_metric_available=gpu_metric_available,
    )

    for group in process_groups:
        process_tasks = group["tasks"]
        process_pods = group["pods"]
        process_pod_names = stringify_unique(record["pod_name"] for record in process_pods)
        process_task_ids = stringify_unique(task["task_id"] for task in process_tasks)
        process_starts = [task["start_date"] for task in process_tasks if task["start_date"] is not None]
        process_ends = [task["end_date"] for task in process_tasks if task["end_date"] is not None]
        process_start = min(process_starts) if process_starts else run_start
        process_end = max(process_ends) if process_ends else run_end
        process_window = build_window(process_start, process_end)
        process_at_expr = prom_at(process_end)
        process_cpu: Optional[float] = None
        process_energy_joules = 0.0
        process_cpu_max_over_time_count = 0
        process_energy_queries: Dict[str, Optional[str]] = {}

        for pod in process_pods:
            pod_name = pod["pod_name"]
            cpu_value, cpu_q = query_cpu_for_pod(
                args.prom_url,
                args.namespace,
                pod_name,
                [(workflow_window, at_expr), (process_window, process_at_expr)],
            )
            if cpu_q and "max_over_time" in cpu_q:
                process_cpu_max_over_time_count += 1
            energy_value, energy_q = try_instant_queries(
                args.prom_url,
                build_energy_queries(args.namespace, pod_name, workflow_window, at_expr),
            )
            if energy_value is None:
                energy_value, energy_q = try_instant_queries(
                    args.prom_url,
                    build_energy_queries(args.namespace, pod_name, process_window, process_at_expr),
                )
            if energy_value is None and detected_energy_metric:
                energy_value, energy_q = query_energy_for_pod(
                    args.prom_url,
                    detected_energy_metric,
                    args.namespace,
                    pod_name,
                    workflow_window,
                    duration_s=seconds_between(run_start, run_end),
                    at_expr=at_expr,
                )
            if energy_value is None and detected_energy_metric:
                energy_value, energy_q = query_energy_for_pod(
                    args.prom_url,
                    detected_energy_metric,
                    args.namespace,
                    pod_name,
                    process_window,
                    duration_s=seconds_between(process_start, process_end),
                    at_expr=process_at_expr,
                )
            if cpu_value is not None:
                process_cpu = (process_cpu or 0.0) + cpu_value
            if energy_value is not None:
                process_energy_joules += energy_value
            process_energy_queries[pod_name] = energy_q

        process_memory = (
            aggregate_memory(
                args.prom_url,
                args.namespace,
                process_pod_names,
                process_start,
                process_end,
                args.memory_step_seconds,
            )
            if process_pod_names
            else {"avg": None, "peak": None}
        )
        process_energy_kwh = joules_to_kwh(process_energy_joules) if process_energy_joules else None
        process_carbon = process_energy_kwh * args.carbon_intensity if process_energy_kwh is not None else None
        process_carbon_method = summarize_carbon_method(args.carbon_intensity, process_energy_kwh is not None)
        process_energy_method, _ = summarize_energy_method(
            detected_energy_metric, process_energy_queries,
            task_count=len(process_task_ids), pod_count=len(process_pod_names),
        )
        process_cpu_method = summarize_cpu_method(
            len(process_task_ids), len(process_pod_names), process_cpu_max_over_time_count
        )
        process_duration_method = summarize_duration_method(len(process_task_ids))
        process_parallelism_note = summarize_parallelism_note(len(process_task_ids), len(process_pod_names))
        process_state = derive_run_state(process_tasks, "None")
        process_memory_avg_gb = bytes_to_gb(process_memory["avg"])
        process_memory_peak_gb = bytes_to_gb(process_memory["peak"])
        process_slug = slugify(
            f"{workflow_name}-{group['slug']}-{run_record['run_id']}-{process_start.isoformat()}"
        )
        process_uri = f"<{base_uri}process/{process_slug}>"
        process_records.append(
            {
                "label": group["label"],
                "uri": process_uri,
                "state": process_state,
                "task_count": len(process_task_ids),
                "pod_count": len(process_pod_names),
                "start": process_start.astimezone(BERLIN_TZ),
                "end": process_end.astimezone(BERLIN_TZ),
                "duration_s": seconds_between(process_start, process_end),
                "cpu": process_cpu,
                "memory_avg_bytes": process_memory["avg"],
                "memory_peak_bytes": process_memory["peak"],
                "memory_avg_gb": process_memory_avg_gb,
                "memory_peak_gb": process_memory_peak_gb,
                "energy_kwh": process_energy_kwh,
                "carbon_kg": process_carbon,
                "carbon_method": process_carbon_method,
                "energy_method": process_energy_method,
                "cpu_method": process_cpu_method,
                "duration_method": process_duration_method,
                "parallelism_note": process_parallelism_note,
            }
        )

    ttl_lines = [
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
        "@prefix dcterms: <http://purl.org/dc/terms/> .",
        "@prefix vivo: <http://vivoweb.org/ontology/core#> .",
        "@prefix bibo: <http://purl.org/ontology/bibo/> .",
        "@prefix prov: <http://www.w3.org/ns/prov#> .",
        f"@prefix rm: <{ontology_uri}> .",
        f"@prefix ex: <{base_uri}> .",
        "@prefix fonda: <https://fonda.hu-berlin.de/ontology#> .",
        "",
        "rm:Workflow",
        "  rdf:type rdfs:Class ;",
        f"  rdfs:label {ttl_literal('Workflow')}@en .",
        "",
        "rm:WorkflowProcessRun",
        "  rdf:type rdfs:Class ;",
        f"  rdfs:label {ttl_literal('workflow stage run')}@en .",
        "",
        "rm:ComputeCluster",
        "  rdf:type rdfs:Class ;",
        f"  rdfs:label {ttl_literal('Compute Cluster')}@en .",
        "",
        "rm:hasWorkflowProcess",
        "  rdf:type rdf:Property ;",
        f"  rdfs:label {ttl_literal('workflow stages')}@en .",
        "",
        "rm:workflow",
        "  rdf:type rdf:Property ;",
        f"  rdfs:label {ttl_literal('workflow name')}@en .",
        "",
        "rm:computeCluster",
        "  rdf:type rdf:Property ;",
        f"  rdfs:label {ttl_literal('compute cluster')}@en .",
        "",
        "rm:traceArchive",
        "  rdf:type rdf:Property ;",
        f"  rdfs:label {ttl_literal('trace archive')}@en .",
        "",
        "rm:traceTypes",
        "  rdf:type rdf:Property ;",
        f"  rdfs:label {ttl_literal('trace types')}@en .",
        "",
        "rm:traceDataFormat",
        "  rdf:type rdf:Property ;",
        f"  rdfs:label {ttl_literal('trace data format')}@en .",
        "",
        "rm:responsibleResearcher",
        "  rdf:type rdf:Property ;",
        f"  rdfs:label {ttl_literal('responsible researcher')}@en .",
        "",
        "rm:ApplicationDomain",
        "  rdf:type rdfs:Class ;",
        f"  rdfs:label {ttl_literal('Application domain')}@en .",
        "",
        "rm:applicationDomain",
        "  rdf:type rdf:Property ;",
        f"  rdfs:label {ttl_literal('application domain')}@en .",
        "",
        "rm:isWorkflowProcessOf",
        "  rdf:type rdf:Property ;",
        f"  rdfs:label {ttl_literal('stage of workflow')}@en .",
        "",
        "rm:carbonIntensitySource",
        "  rdf:type rdf:Property ;",
        f"  rdfs:label {ttl_literal('carbon intensity source')}@en .",
        "",
        "rm:WorkflowEngine",
        "  rdf:type rdfs:Class ;",
        f"  rdfs:label {ttl_literal('Workflow Engine')}@en .",
        "",
        "rm:Language",
        "  rdf:type rdfs:Class ;",
        f"  rdfs:label {ttl_literal('Language')}@en .",
        "",
        # --- Workflow engine entity ---
        f"{engine_uri}",
        "  rdf:type rm:WorkflowEngine ;",
        "  rdf:type vivo:InformationResource ;",
        f"  rdfs:label {ttl_literal(args.workflow_engine)}@en ;",
        f"  dcterms:title {ttl_literal(args.workflow_engine)}@en .",
        "",
        # --- Language entity ---
        *(
            [
                f"{language_uri}",
                "  rdf:type rm:Language ;",
                "  rdf:type vivo:InformationResource ;",
                f"  rdfs:label {ttl_literal(args.language)}@en ;",
                f"  dcterms:title {ttl_literal(args.language)}@en .",
                "",
            ]
            if language_uri else []
        ),
        # --- Cluster entity ---
        f"{cluster_uri}",
        "  rdf:type rm:ComputeCluster ;",
        "  rdf:type vivo:InformationResource ;",
        f"  rdfs:label {ttl_literal(args.cluster_label)}@en ;",
        f"  dcterms:title {ttl_literal(args.cluster_label)}@en ;",
        f"  rm:hasRun {run_uri} .",
        "",
        # --- Workflow entity ---
        f"{workflow_entity_uri}",
        "  rdf:type rm:Workflow ;",
        "  rdf:type vivo:InformationResource ;",
        "  rdf:type prov:Entity ;",
        f"  rdfs:label {ttl_literal(public_workflow_name or workflow_name)}@en ;",
        f"  dcterms:title {ttl_literal(public_workflow_name or workflow_name)}@en ;",
        f"  rm:workflowName {ttl_literal(public_workflow_name or workflow_name)} ;",
    ]
    if args.code_uri:
        ttl_lines.append(f"  rm:workflowCodeLink {ttl_literal(args.code_uri, 'xsd:anyURI')} ;")
    if workflow_ui_link:
        ttl_lines.append(f"  rm:workflowUiLink {ttl_literal(workflow_ui_link, 'xsd:anyURI')} ;")
    if args.trace_archive:
        ttl_lines.append(f"  rm:traceArchive {ttl_literal(args.trace_archive, 'xsd:anyURI')} ;")
    if args.trace_types:
        ttl_lines.append(f"  rm:traceTypes {ttl_literal(args.trace_types)} ;")
    if args.trace_data_format:
        ttl_lines.append(f"  rm:traceDataFormat {ttl_literal(args.trace_data_format)} ;")
    if args.responsible_researcher_uri:
        ttl_lines.append(f"  rm:responsibleResearcher {as_ttl_uri(args.responsible_researcher_uri)} ;")
    if args.application_domain_uri:
        ttl_lines.append(f"  rm:applicationDomain {as_ttl_uri(args.application_domain_uri)} ;")
    ttl_lines.extend([
        f"  rm:describesSoftwareExecution {software_uri} ;",
        f"  rm:hasRun {run_uri} ;",
        f"  rm:hasWorkflowRun {run_uri} .",
        "",
        # --- Publication / software resource: link to this run ---
        f"{software_uri}",
        f"  rm:hasRun {run_uri} .",
        "",
        # --- Run entity ---
        f"{run_uri}",
        "  rdf:type rm:RunMetadata ;",
        "  rdf:type vivo:InformationResource ;",
        "  rdf:type prov:Entity ;",
        f"  rdfs:label {ttl_literal(metadata_title)}@en ;",
        f"  dcterms:title {ttl_literal(metadata_title)}@en ;",
        f"  rm:describesSoftwareExecution {software_uri} ;",
        f"  rm:workflow {workflow_entity_uri} ;",
        f"  rm:computeCluster {cluster_uri} ;",
        f"  rm:codeVersion {ttl_literal(code_version)} ;",
        f"  rm:runStatus {ttl_literal(normalize_state(effective_state))} ;",
        f"  rm:workflowEngine {engine_uri} ;",
        *(
            [f"  rm:language {language_uri} ;"]
            if language_uri else []
        ),
        f"  rm:backend {ttl_literal(backend)} ;",
        f"  rm:startTime {ttl_literal(run_start_local_naive, 'xsd:dateTime')} ;",
        f"  rm:endTime {ttl_literal(run_end_local_naive, 'xsd:dateTime')} ;",
        f"  rm:durationSeconds {ttl_literal(duration_s, 'xsd:integer')} ;",
        f"  rm:durationCalculationMethod {ttl_literal(overall_duration_method)} ;",
        f"  dcterms:created {ttl_literal(metadata_generated_at.replace(microsecond=0).isoformat(), 'xsd:dateTime')} ;",
        f"  rm:jobName {ttl_literal(args.dag_id)} ;",
        f"  rm:cpuTimeSeconds {ttl_literal(total_cpu)} ;",
        f"  rm:cpuTimeCalculationMethod {ttl_literal(overall_cpu_method)} ;",
    ])

    if memory_peak_gb is not None:
        ttl_lines.append(f"  rm:memoryPeakGB {ttl_literal(memory_peak_gb)} ;")
    if memory_avg_gb is not None:
        ttl_lines.append(f"  rm:memoryAvgGB {ttl_literal(memory_avg_gb)} ;")
    if energy_kwh is not None:
        ttl_lines.append(f"  rm:energyKWh {ttl_literal(energy_kwh)} ;")
    if carbon_kg is not None:
        ttl_lines.append(f"  rm:carbonEmissionKgCO2e {ttl_literal(carbon_kg)} ;")

    ttl_lines.append(f"  rm:carbonIntensityAssumptionKgCO2ePerKWh {ttl_literal(args.carbon_intensity)} ;")
    ttl_lines.append(f"  rm:carbonIntensitySource {ttl_literal(carbon_intensity_source)} ;")
    ttl_lines.append(f"  rm:energyMetricSource {ttl_literal(detected_energy_metric or 'none')} ;")
    ttl_lines.append(f"  rm:energyCalculationMethod {ttl_literal(energy_method)} ;")
    ttl_lines.append(f"  rm:energyCalculationUsesFallbackEstimate {ttl_bool(energy_method_uses_fallback)} ;")
    ttl_lines.append(f"  rm:carbonCalculationMethod {ttl_literal(carbon_method)} ;")
    ttl_lines.append(f"  rm:gpuRequested {ttl_bool(gpu_requested)} ;")
    ttl_lines.append(f"  rm:gpuMetricsAvailable {ttl_bool(gpu_metric_available)} ;")
    ttl_lines.append(f"  rm:gpuCapableNodeUsed {ttl_bool(gpu_capable_node_used)} ;")
    ttl_lines.append(f"  rm:gpuUsageStatus {ttl_literal(gpu_usage_status)} ;")

    for image in unique_images:
        ttl_lines.append(f"  rm:containerImage {ttl_literal(image)} ;")
    for host in execution_hosts:
        ttl_lines.append(f"  rm:executionHost {ttl_literal(host)} ;")
    for node_name in unique_nodes:
        ttl_lines.append(f"  rm:nodeName {ttl_literal(node_name)} ;")
    for value in allocatable_cpus:
        ttl_lines.append(f"  rm:allocatableCpu {ttl_literal(value)} ;")
    for value in allocatable_mem_gbs:
        ttl_lines.append(f"  rm:allocatableMemoryGB {ttl_literal(value)} ;")
    for value in architectures:
        ttl_lines.append(f"  rm:architecture {ttl_literal(value)} ;")
    for value in kernel_versions:
        ttl_lines.append(f"  rm:kernelVersion {ttl_literal(value)} ;")
    for value in kubelet_versions:
        ttl_lines.append(f"  rm:kubeletVersion {ttl_literal(value)} ;")
    for summary in gpu_node_summaries:
        ttl_lines.append(f"  rm:gpuHardwareSummary {ttl_literal(summary)} ;")
    for key in gpu_request_keys:
        ttl_lines.append(f"  rm:gpuResourceKey {ttl_literal(key)} ;")
    for process in process_records:
        ttl_lines.append(f"  rm:hasWorkflowProcess {process['uri']} ;")
    if args.code_uri:
        ttl_lines.append(f"  rm:workflowCodeLink {ttl_literal(args.code_uri, 'xsd:anyURI')} ;")
    if workflow_ui_link:
        ttl_lines.append(f"  rm:workflowUiLink {ttl_literal(workflow_ui_link, 'xsd:anyURI')} ;")

    last_os_image = os_images[-1] if os_images else ""
    ttl_lines.append(f"  rm:osImage {ttl_literal(last_os_image)} .")

    for process in process_records:
        ttl_lines.extend([
            "",
            f"{process['uri']}",
            "  rdf:type rm:WorkflowProcessRun ;",
            "  rdf:type vivo:InformationResource ;",
            "  rdf:type prov:Entity ;",
            f"  rdfs:label {ttl_literal(process['label'])}@en ;",
            f"  dcterms:title {ttl_literal(process['label'])}@en ;",
            f"  rm:jobName {ttl_literal(process['label'])} ;",
            f"  rm:runStatus {ttl_literal(normalize_state(process['state']))} ;",
            f"  rm:taskCount {ttl_literal(process['task_count'], 'xsd:integer')} ;",
            f"  rm:podCount {ttl_literal(process['pod_count'], 'xsd:integer')} ;",
            f"  rm:startTime {ttl_literal(naive_local_iso(process['start']), 'xsd:dateTime')} ;",
            f"  rm:endTime {ttl_literal(naive_local_iso(process['end']), 'xsd:dateTime')} ;",
            f"  rm:durationSeconds {ttl_literal(process['duration_s'], 'xsd:integer')} ;",
        ])
        if process["cpu"] is not None:
            ttl_lines.append(f"  rm:cpuTimeSeconds {ttl_literal(process['cpu'])} ;")
        if process["memory_peak_gb"] is not None:
            ttl_lines.append(f"  rm:memoryPeakGB {ttl_literal(process['memory_peak_gb'])} ;")
        if process["memory_avg_gb"] is not None:
            ttl_lines.append(f"  rm:memoryAvgGB {ttl_literal(process['memory_avg_gb'])} ;")
        if process["energy_kwh"] is not None:
            ttl_lines.append(f"  rm:energyKWh {ttl_literal(process['energy_kwh'])} ;")
        if process["carbon_kg"] is not None:
            ttl_lines.append(f"  rm:carbonEmissionKgCO2e {ttl_literal(process['carbon_kg'])} ;")
        ttl_lines.append(f"  rm:carbonCalculationMethod {ttl_literal(process['carbon_method'])} ;")
        ttl_lines.append(f"  rm:energyCalculationMethod {ttl_literal(process['energy_method'])} ;")
        ttl_lines.append(f"  rm:carbonIntensityAssumptionKgCO2ePerKWh {ttl_literal(args.carbon_intensity)} ;")
        ttl_lines.append(f"  rm:cpuTimeCalculationMethod {ttl_literal(process['cpu_method'])} ;")
        ttl_lines.append(f"  rm:durationCalculationMethod {ttl_literal(process['duration_method'])} ;")
        ttl_lines.append(f"  rm:parallelismNote {ttl_literal(process['parallelism_note'])} .")

    with open(args.output_file, "w", encoding="utf-8") as handle:
        handle.write("\n".join(ttl_lines) + "\n")

    print(f"Wrote {args.output_file}")
    print(f"workflow_name={workflow_name}")
    print(f"run_id={run_record['run_id']}")
    print(f"execution_date={run_record['execution_date']}")
    print(f"effective_state={effective_state}")
    print(f"excluded_task_ids={','.join(excluded_task_ids) if excluded_task_ids else '(none)'}")
    print(f"workflow_start={run_start.isoformat()}")
    print(f"workflow_end={run_end.isoformat()}")
    print(f"pod_count={len(pod_names)}")
    print(f"workflow_engine={args.workflow_engine}")
    print(f"language={args.language or '(not set)'}")
    print(f"backend={backend}")
    print(f"execution_hosts={','.join(execution_hosts) if execution_hosts else '(none)'}")
    print(f"workflow_ui_link={workflow_ui_link or '(none)'}")
    print(f"cpu_seconds={total_cpu}")
    print(f"memory_avg_gb={memory_avg_gb}")
    print(f"memory_peak_gb={memory_peak_gb}")
    print(f"gpu_requested={gpu_requested}")
    print(f"gpu_metrics_available={gpu_metric_available}")
    print(f"gpu_capable_node_used={gpu_capable_node_used}")
    print(f"gpu_usage_status={gpu_usage_status}")
    print(f"energy_metric={detected_energy_metric}")
    print(f"energy_calculation_uses_fallback_estimate={energy_method_uses_fallback}")
    print(f"energy_calculation_method={energy_method}")
    print(f"carbon_calculation_method={carbon_method}")
    print(f"energy_kwh={energy_kwh}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
