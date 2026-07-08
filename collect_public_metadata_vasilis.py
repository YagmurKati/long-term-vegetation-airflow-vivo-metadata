#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


PROM_URL_DEFAULT = "http://localhost:9090"
CARBON_INTENSITY_DEFAULT = 0.4
BASE_URI_DEFAULT = "http://example.org/vivo-import/run-metadata/"
ONTOLOGY_URI_DEFAULT = "http://example.org/ontology/run-metadata#"


def run_cmd(cmd: List[str]) -> str:
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
      raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{p.stderr}")
    return p.stdout.strip()


def kubectl_json(args: List[str]) -> Dict[str, Any]:
    out = run_cmd(["kubectl"] + args + ["-o", "json"])
    return json.loads(out)


def parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def slugify(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "item"


def ttl_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")


def ttl_literal(value: Any, datatype: Optional[str] = None) -> str:
    if isinstance(value, bool):
        return f"\"{'true' if value else 'false'}\"^^xsd:boolean"
    if isinstance(value, int):
        return f"\"{value}\"^^{datatype or 'xsd:int'}"
    if isinstance(value, float):
        return f"\"{value}\"^^{datatype or 'xsd:double'}"
    escaped = ttl_escape(str(value))
    return f"\"{escaped}\"" if datatype is None else f"\"{escaped}\"^^{datatype}"


def as_ttl_uri(value: str) -> str:
    if value.startswith(("http://", "https://", "urn:")):
        return f"<{value}>"
    return value


def http_get_json(url: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def prom_query(prom_url: str, query: str) -> Any:
    data = http_get_json(f"{prom_url}/api/v1/query", {"query": query})
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed:\n{query}\n{data}")
    return data["data"]["result"]


def first_value(result: Any) -> Optional[float]:
    if not result:
        return None
    try:
        return float(result[0]["value"][1])
    except Exception:
        return None


def seconds_between(start: datetime, end: datetime) -> int:
    return max(1, int((end - start).total_seconds()))


def prom_at(ts: Optional[datetime]) -> str:
    if ts is None:
        return ""
    return f" @ {int(ts.timestamp())}"


def build_window(start: datetime, end: datetime) -> str:
    return f"[{seconds_between(start, end)}s]"


def try_queries(prom_url: str, queries: List[str]) -> Dict[str, Any]:
    for q in queries:
        try:
            res = prom_query(prom_url, q)
            val = first_value(res)
            if val is not None:
                return {"value": val, "query": q}
        except Exception:
            pass
    return {"value": None, "query": None}


def get_pod_name_from_job(namespace: str, job_name: str) -> str:
    data = kubectl_json(["get", "pods", "-n", namespace, "-l", f"job-name={job_name}"])
    items = data.get("items", [])
    if not items:
        raise RuntimeError(f"No pod found for job '{job_name}' in namespace '{namespace}'")
    items.sort(key=lambda x: x["metadata"]["creationTimestamp"])
    return items[-1]["metadata"]["name"]


def get_container_spec(pod: Dict[str, Any], requested_name: Optional[str]) -> Dict[str, Any]:
    containers = pod.get("spec", {}).get("containers", [])
    if not containers:
        raise RuntimeError("Pod has no containers")
    if requested_name:
        for container in containers:
            if container.get("name") == requested_name:
                return container
        raise RuntimeError(f"Container '{requested_name}' not found in pod")
    return containers[0]


def get_container_status(pod: Dict[str, Any], container_name: str) -> Dict[str, Any]:
    statuses = pod.get("status", {}).get("containerStatuses", [])
    for status in statuses:
        if status.get("name") == container_name:
            return status
    return statuses[0] if statuses else {}


def get_job(namespace: str, job_name: str) -> Dict[str, Any]:
    return kubectl_json(["get", "job", job_name, "-n", namespace])


def get_run_times(job: Dict[str, Any], pod: Dict[str, Any], container_status: Dict[str, Any]) -> Dict[str, Any]:
    status = pod.get("status", {})
    pod_start = parse_ts(status.get("startTime"))
    phase = status.get("phase")
    state = container_status.get("state", {})

    started_at = None
    finished_at = None
    exit_code = None

    if "terminated" in state:
        term = state["terminated"]
        started_at = parse_ts(term.get("startedAt"))
        finished_at = parse_ts(term.get("finishedAt"))
        exit_code = term.get("exitCode")

    if started_at is None:
        started_at = parse_ts(job.get("status", {}).get("startTime")) or pod_start
    if finished_at is None:
        finished_at = parse_ts(job.get("status", {}).get("completionTime"))
    if finished_at is None:
        finished_at = datetime.now(timezone.utc)

    return {
        "phase": phase,
        "started_at": started_at,
        "finished_at": finished_at,
        "exit_code": exit_code,
    }


def build_cpu_queries(namespace: str, pod_name: str, job_name: str, container_name: str, window: str, at_expr: str) -> List[str]:
    return [
        f'sum(increase(container_cpu_usage_seconds_total{{namespace="{namespace}",pod="{pod_name}",container="{container_name}"}}{window}{at_expr}))',
        f'sum(increase(container_cpu_usage_seconds_total{{namespace="{namespace}",pod="{pod_name}",container!="",image!=""}}{window}{at_expr}))',
        f'sum(increase(container_cpu_usage_seconds_total{{namespace="{namespace}",pod=~"{job_name}.*",container="{container_name}"}}{window}{at_expr}))',
        f'sum(increase(container_cpu_usage_seconds_total{{namespace="{namespace}",pod=~"{job_name}.*",container!="",image!=""}}{window}{at_expr}))',
    ]


def build_memory_peak_queries(namespace: str, pod_name: str, job_name: str, container_name: str, window: str, at_expr: str) -> List[str]:
    return [
        f'max_over_time(container_memory_working_set_bytes{{namespace="{namespace}",pod="{pod_name}",container="{container_name}"}}{window}{at_expr})',
        f'max_over_time(container_memory_working_set_bytes{{namespace="{namespace}",pod="{pod_name}",container!="",image!=""}}{window}{at_expr})',
        f'max_over_time(container_memory_working_set_bytes{{namespace="{namespace}",pod=~"{job_name}.*",container="{container_name}"}}{window}{at_expr})',
        f'max_over_time(container_memory_working_set_bytes{{namespace="{namespace}",pod=~"{job_name}.*",container!="",image!=""}}{window}{at_expr})',
    ]


def build_memory_avg_queries(namespace: str, pod_name: str, job_name: str, container_name: str, window: str, at_expr: str) -> List[str]:
    return [
        f'avg_over_time(container_memory_working_set_bytes{{namespace="{namespace}",pod="{pod_name}",container="{container_name}"}}{window}{at_expr})',
        f'avg_over_time(container_memory_working_set_bytes{{namespace="{namespace}",pod="{pod_name}",container!="",image!=""}}{window}{at_expr})',
        f'avg_over_time(container_memory_working_set_bytes{{namespace="{namespace}",pod=~"{job_name}.*",container="{container_name}"}}{window}{at_expr})',
        f'avg_over_time(container_memory_working_set_bytes{{namespace="{namespace}",pod=~"{job_name}.*",container!="",image!=""}}{window}{at_expr})',
    ]


def build_energy_queries(namespace: str, pod_name: str, job_name: str, container_name: str, window: str, at_expr: str) -> List[str]:
    return [
        f'sum(increase(kepler_container_joules_total{{namespace="{namespace}",pod_name="{pod_name}",container_name="{container_name}"}}{window}{at_expr}))',
        f'sum(increase(kepler_container_joules_total{{namespace="{namespace}",pod_name="{pod_name}"}}{window}{at_expr}))',
        f'sum(increase(kepler_container_joules_total{{namespace="{namespace}",pod="{pod_name}"}}{window}{at_expr}))',
        f'sum(increase(kepler_container_joules_total{{pod_name=~"{job_name}.*"}}{window}{at_expr}))',
        f'sum(increase(kepler_container_package_joules_total{{pod_name=~"{job_name}.*"}}{window}{at_expr}))',
    ]


def joules_to_kwh(joules: float) -> float:
    return joules / 3_600_000.0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Collect VIVO-friendly metadata for one Kubernetes benchmark run.")
    p.add_argument("--namespace", required=True)
    p.add_argument("--job-name", required=True)
    p.add_argument("--pod-name", default=None)
    p.add_argument("--container-name", default=None)
    p.add_argument("--code-name", required=True)
    p.add_argument("--code-path", required=True)
    p.add_argument("--output-file", required=True)
    p.add_argument("--prom-url", default=PROM_URL_DEFAULT)
    p.add_argument("--base-uri", default=BASE_URI_DEFAULT)
    p.add_argument("--ontology-uri", default=ONTOLOGY_URI_DEFAULT)
    p.add_argument("--software-uri", default=None)
    p.add_argument("--software-title", default=None)
    p.add_argument("--carbon-intensity", type=float, default=CARBON_INTENSITY_DEFAULT)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    pod_name = args.pod_name or get_pod_name_from_job(args.namespace, args.job_name)
    job = get_job(args.namespace, args.job_name)
    pod = kubectl_json(["get", "pod", pod_name, "-n", args.namespace])
    container_spec = get_container_spec(pod, args.container_name)
    container_name = container_spec["name"]
    container_status = get_container_status(pod, container_name)

    run_times = get_run_times(job, pod, container_status)
    started_at = run_times["started_at"]
    finished_at = run_times["finished_at"]
    phase = run_times["phase"]

    if started_at is None or finished_at is None:
        raise RuntimeError("Could not determine run start and finish times")

    duration_s = seconds_between(started_at, finished_at)
    window = build_window(started_at, finished_at)
    at_expr = prom_at(finished_at)

    code_hash = sha256_file(args.code_path)
    code_version = f"sha256:{code_hash}"

    node_name = pod["spec"].get("nodeName")
    image = container_spec.get("image")
    node = kubectl_json(["get", "node", node_name])
    node_info = node.get("status", {}).get("nodeInfo", {})
    allocatable = node.get("status", {}).get("allocatable", {})

    cpu = try_queries(args.prom_url, build_cpu_queries(args.namespace, pod_name, args.job_name, container_name, window, at_expr))
    mem_peak = try_queries(args.prom_url, build_memory_peak_queries(args.namespace, pod_name, args.job_name, container_name, window, at_expr))
    mem_avg = try_queries(args.prom_url, build_memory_avg_queries(args.namespace, pod_name, args.job_name, container_name, window, at_expr))
    energy = try_queries(args.prom_url, build_energy_queries(args.namespace, pod_name, args.job_name, container_name, window, at_expr))

    energy_joules = energy["value"]
    energy_kwh = joules_to_kwh(energy_joules) if energy_joules is not None else None
    carbon_kg = energy_kwh * args.carbon_intensity if energy_kwh is not None else None

    base_uri = args.base_uri.rstrip("/") + "/"
    ontology_uri = args.ontology_uri if args.ontology_uri.endswith(("#", "/")) else args.ontology_uri + "#"
    run_slug = slugify(f"{args.namespace}-{args.job_name}-{pod_name}-{started_at.isoformat()}")
    run_uri = f"<{base_uri}run/{run_slug}>"
    dt_uri = f"<{base_uri}datetime/{run_slug}>"

    software_uri = as_ttl_uri(args.software_uri) if args.software_uri else f"<{base_uri}software/{slugify(args.code_name)}>"
    metadata_title = f"Execution metadata for {args.code_name}"

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
        "",
    ]

    if not args.software_uri:
        label = args.software_title or args.code_name
        ttl_lines.extend([
            f"{software_uri}",
            "  rdf:type bibo:Software ;",
            f"  rdfs:label {ttl_literal(label)}@en ;",
            f"  dcterms:title {ttl_literal(label)}@en ;",
            f"  vivo:produces {run_uri} .",
            "",
        ])

    ttl_lines.extend([
        f"{run_uri}",
        "  rdf:type rm:RunMetadata ;",
        "  rdf:type vivo:InformationResource ;",
        "  rdf:type prov:Entity ;",
        f"  rdfs:label {ttl_literal(metadata_title)}@en ;",
        f"  dcterms:title {ttl_literal(metadata_title)}@en ;",
        f"  rm:describesSoftwareExecution {software_uri} ;",
        f"  rm:codeName {ttl_literal(args.code_name)} ;",
        f"  rm:codeVersion {ttl_literal(code_version)} ;",
        f"  rm:runStatus {ttl_literal(phase)} ;",
        f"  rm:durationSeconds {ttl_literal(duration_s, 'xsd:integer')} ;",
        f"  dcterms:created {ttl_literal(finished_at.replace(microsecond=0).isoformat(), 'xsd:dateTime')} ;",
        f"  vivo:dateTimeValue {dt_uri} ;",
    ])

    if cpu["value"] is not None:
        ttl_lines.append(f"  rm:cpuTimeSeconds {ttl_literal(cpu['value'])} ;")
    if mem_peak["value"] is not None:
        ttl_lines.append(f"  rm:memoryPeakBytes {ttl_literal(mem_peak['value'])} ;")
    if mem_avg["value"] is not None:
        ttl_lines.append(f"  rm:memoryAvgBytes {ttl_literal(mem_avg['value'])} ;")
    if energy_kwh is not None:
        ttl_lines.append(f"  rm:energyKWh {ttl_literal(energy_kwh)} ;")
    if carbon_kg is not None:
        ttl_lines.append(f"  rm:carbonEmissionKgCO2e {ttl_literal(carbon_kg)} ;")

    ttl_lines.extend([
        f"  rm:carbonIntensityAssumptionKgCO2ePerKWh {ttl_literal(args.carbon_intensity)} ;",
        f"  rm:containerImage {ttl_literal(image)} ;",
        f"  rm:jobName {ttl_literal(args.job_name)} ;",
        f"  rm:allocatableCpu {ttl_literal(str(allocatable.get('cpu')))} ;",
        f"  rm:allocatableMemory {ttl_literal(str(allocatable.get('memory')))} ;",
        f"  rm:architecture {ttl_literal(str(node_info.get('architecture')))} ;",
        f"  rm:kernelVersion {ttl_literal(str(node_info.get('kernelVersion')))} ;",
        f"  rm:kubeletVersion {ttl_literal(str(node_info.get('kubeletVersion')))} ;",
        f"  rm:osImage {ttl_literal(str(node_info.get('osImage')))} .",
        "",
        f"{software_uri}",
        f"  rm:hasWorkflowRun {run_uri} .",
        "",
        f"{dt_uri}",
        "  rdf:type vivo:DateTimeValue ;",
        f"  vivo:dateTime {ttl_literal(finished_at.replace(microsecond=0).isoformat(), 'xsd:dateTime')} ;",
        "  vivo:dateTimePrecision vivo:yearMonthDayPrecision .",
    ])

    with open(args.output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(ttl_lines) + "\n")

    print(f"Wrote {args.output_file}")
    print(f"pod={pod_name}")
    print(f"container={container_name}")
    print(f"cpu_query={cpu['query']}")
    print(f"mem_peak_query={mem_peak['query']}")
    print(f"mem_avg_query={mem_avg['query']}")
    print(f"energy_query={energy['query']}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
