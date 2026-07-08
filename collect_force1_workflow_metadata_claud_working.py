#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo


DEFAULT_WORKFLOW_NAME = "Long-term vegetation dynamics in the Mediterranean"
DEFAULT_DAG_ID = "force-1"
DEFAULT_WORKFLOW_ENGINE = "Apache Airflow"
DEFAULT_AIRFLOW_NAMESPACE = "airflow-yagmur"
DEFAULT_EXECUTION_NAMESPACE = "default"
DEFAULT_EXCLUDE_TASK_IDS = "check_results"
DEFAULT_PUBLICATION_URI = (
    "http://141.20.184.157:8080/vivo/individual/"
    "a1-publication-doi-10-1093-gigascience-giae030"
)
DEFAULT_CODE_URI = (
    "https://github.com/YagmurKati/fonda-airflow-dags/blob/"
    "codex/yagmur-airflow-import-fix/dags/s1/force/workflow.py"
)
DEFAULT_TRACE_ARCHIVE = "https://box.hu-berlin.de/f/c4d90fc5b07c4955b979/"
DEFAULT_TRACE_TYPES = "Airflow Logs, Force logs"
DEFAULT_TRACE_DATA_FORMAT = "Zipped text files"
DEFAULT_RESPONSIBLE_RESEARCHER_URI = "https://fonda.hu-berlin.de/?page_id=2066#VasilisBountris"
DEFAULT_APPLICATION_DOMAIN_URI = "http://172.28.33.178:8080/vivo/individual/n5261"
DEFAULT_ELECTRICITYMAP_TOKEN = "dPnOScEsmerLLvMDgXOD"
DEFAULT_WORKFLOW_UI_BASE_URL = "http://127.0.0.1:8080"
VERSIONED_OUTPUT_STEM = "long-term-vegetation-dynamics-mediterranean-workflow-public-metadata-with-processes"
TIMESTAMPED_OUTPUT_STEM = "long-term-vegetation-dynamics-mediterranean-workflow-public-metadata"
DEFAULT_OUTPUT_SUBDIR = "claudttl"
BERLIN_TZ = ZoneInfo("Europe/Berlin")

EXTENSION_TO_LANGUAGE = {
    ".py": "Python",
    ".r": "R",
    ".R": "R",
    ".sh": "Shell",
    ".bash": "Shell",
    ".snakefile": "Snakemake",
    ".smk": "Snakemake",
    ".nf": "Nextflow",
    ".wdl": "WDL",
    ".cwl": "CWL",
}


def detect_language(code_path: str) -> str:
    suffix = Path(code_path).suffix
    return EXTENSION_TO_LANGUAGE.get(suffix, "Unknown")


def run_cmd(args: List[str]) -> str:
    proc = subprocess.run(args, check=True, text=True, capture_output=True)
    return proc.stdout


def parse_airflow_table(output: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    headers: Optional[List[str]] = None
    for raw_line in output.splitlines():
        if "|" not in raw_line:
            continue
        parts = [part.strip() for part in raw_line.split("|")]
        if headers is None:
            headers = parts
            continue
        if all(set(part) <= {"=", "+"} for part in parts):
            continue
        if len(parts) != len(headers):
            continue
        rows.append(dict(zip(headers, parts)))
    return rows


def detect_scheduler_pod(airflow_namespace: str) -> str:
    output = run_cmd(
        [
            "kubectl",
            "-n",
            airflow_namespace,
            "get",
            "pod",
            "-l",
            "component=scheduler",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ]
    ).strip()
    if not output:
        raise RuntimeError(f"No scheduler pod found in namespace '{airflow_namespace}'")
    return output


def latest_finished_run_id(airflow_namespace: str, scheduler_pod: str, dag_id: str) -> str:
    rows = finished_runs(airflow_namespace, scheduler_pod, dag_id)
    if rows:
        run_id = rows[0].get("run_id", "").strip()
        if run_id:
            return run_id
    raise RuntimeError(f"No finished run found for dag_id '{dag_id}'")


def finished_runs(airflow_namespace: str, scheduler_pod: str, dag_id: str) -> List[Dict[str, str]]:
    output = run_cmd(
        [
            "kubectl",
            "-n",
            airflow_namespace,
            "exec",
            scheduler_pod,
            "--",
            "airflow",
            "dags",
            "list-runs",
            "-d",
            dag_id,
        ]
    )
    rows: List[Dict[str, str]] = []
    for row in parse_airflow_table(output):
        state = row.get("state", "").strip().lower()
        if state in {"success", "failed"}:
            rows.append(row)
    return rows


def timestamped_output_path(output_dir: Path, stem: str, output_stamp: Optional[str]) -> Path:
    stamp = output_stamp or datetime.now(BERLIN_TZ).strftime("%d%m%Y_%H%M")
    return output_dir / f"{stem}-{stamp}.ttl"


def write_manifest(output_dir: Path, files: List[Path]) -> Path:
    manifest_path = output_dir / "VIVO_UPLOAD_FILES.txt"
    with open(manifest_path, "w", encoding="utf-8") as handle:
        handle.write("TTL files ready to upload to VIVO:\n")
        for path in files:
            handle.write(f"{path}\n")
    return manifest_path


def parse_optional_datetime(value: Optional[str]) -> Optional[datetime]:
    text = (value or "").strip()
    if not text or text == "None":
        return None
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def run_stamp_for_row(row: Dict[str, str]) -> str:
    dt = (
        parse_optional_datetime(row.get("start_date"))
        or parse_optional_datetime(row.get("execution_date"))
        or datetime.now(BERLIN_TZ)
    )
    return dt.astimezone(BERLIN_TZ).strftime("%d%m%Y_%H%M")


def build_args() -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description=(
            "Collect workflow metadata for the latest finished force-1 Airflow run and "
            "write a timestamped TTL file automatically."
        )
    )
    parser.add_argument("--airflow-namespace", default=DEFAULT_AIRFLOW_NAMESPACE)
    parser.add_argument("--namespace", default=DEFAULT_EXECUTION_NAMESPACE)
    parser.add_argument("--dag-id", default=DEFAULT_DAG_ID)
    parser.add_argument("--run-id", default=None, help="Optional explicit run id to use")
    parser.add_argument("--workflow-name", default=DEFAULT_WORKFLOW_NAME)
    parser.add_argument("--workflow-engine", default=DEFAULT_WORKFLOW_ENGINE)
    parser.add_argument("--code-name", default=DEFAULT_WORKFLOW_NAME)
    parser.add_argument("--code-path", default=str(here / "fonda-airflow-dags-yagmur" / "dags" / "s1" / "force" / "workflow.py"))
    parser.add_argument("--code-uri", default=DEFAULT_CODE_URI)
    parser.add_argument("--workflow-ui-base-url", default=DEFAULT_WORKFLOW_UI_BASE_URL)
    parser.add_argument("--software-title", default=DEFAULT_WORKFLOW_NAME)
    parser.add_argument("--publication-uri", default=DEFAULT_PUBLICATION_URI)
    parser.add_argument("--exclude-task-ids", default=DEFAULT_EXCLUDE_TASK_IDS)
    parser.add_argument("--prom-url", default="http://127.0.0.1:9090")
    parser.add_argument("--output-dir", default=str(here / DEFAULT_OUTPUT_SUBDIR))
    parser.add_argument(
        "--all-runs",
        action="store_true",
        help="Collect metadata for all finished runs of the selected DAG, writing one timestamped TTL per run",
    )
    parser.add_argument(
        "--output-stamp",
        default=None,
        help="Optional Berlin-time stamp for the output filename, for example 06052026_1411",
    )
    parser.add_argument("--trace-archive", default=DEFAULT_TRACE_ARCHIVE)
    parser.add_argument(
        "--trace-types",
        default=DEFAULT_TRACE_TYPES,
        help="Free-text kinds of traces in the archive (e.g. 'Airflow Logs, Force logs')",
    )
    parser.add_argument(
        "--trace-data-format",
        default=DEFAULT_TRACE_DATA_FORMAT,
        help="Free-text trace data format (e.g. 'Zipped text files')",
    )
    parser.add_argument(
        "--responsible-researcher-uri",
        default=DEFAULT_RESPONSIBLE_RESEARCHER_URI,
        help="URI of the responsible researcher individual",
    )
    parser.add_argument(
        "--application-domain-uri",
        default=DEFAULT_APPLICATION_DOMAIN_URI,
        help="URI of the Application domain individual (e.g. Remote sensing)",
    )
    parser.add_argument("--cluster-label", default="Fonda Cluster")
    parser.add_argument("--cluster-slug", default="fonda-cluster")
    parser.add_argument(
        "--language",
        default=None,
        help="Programming language of the workflow (auto-detected from code-path if not set)",
    )
    parser.add_argument(
        "--electricitymap-token",
        default=os.environ.get("ELECTRICITYMAP_TOKEN", DEFAULT_ELECTRICITYMAP_TOKEN),
        help="ElectricityMaps API token (defaults to ELECTRICITYMAP_TOKEN env var)",
    )
    parser.add_argument("--electricitymap-zone", default="DE")
    parser.add_argument(
        "--collector-path",
        default=str(here / "collect_airflow_workflow_metadata_vasilis_claud_working.py"),
    )
    return parser.parse_args()


def main() -> None:
    args = build_args()
    language = args.language or detect_language(args.code_path)
    scheduler_pod = detect_scheduler_pod(args.airflow_namespace)
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.all_runs:
        rows = finished_runs(args.airflow_namespace, scheduler_pod, args.dag_id)
        if not rows:
            raise RuntimeError(f"No finished run found for dag_id '{args.dag_id}'")
        print(f"scheduler_pod={scheduler_pod}")
        print(f"run_count={len(rows)}")
        generated_files: List[Path] = []
        for row in rows:
            run_id = row.get("run_id", "").strip()
            if not run_id:
                continue
            output_path = timestamped_output_path(
                output_dir,
                TIMESTAMPED_OUTPUT_STEM,
                run_stamp_for_row(row),
            )
            cmd = [
                sys.executable,
                args.collector_path,
                "--airflow-namespace",
                args.airflow_namespace,
                "--namespace",
                args.namespace,
                "--dag-id",
                args.dag_id,
                "--run-id",
                run_id,
                "--exclude-task-ids",
                args.exclude_task_ids,
                "--workflow-name",
                args.workflow_name,
                "--workflow-engine",
                args.workflow_engine,
                "--code-name",
                args.code_name,
                "--code-path",
                args.code_path,
                "--code-uri",
                args.code_uri,
                "--workflow-ui-base-url",
                args.workflow_ui_base_url,
                "--output-file",
                str(output_path),
                "--software-title",
                args.software_title,
                "--publication-uri",
                args.publication_uri,
                "--prom-url",
                args.prom_url,
                "--trace-archive",
                args.trace_archive,
                "--responsible-researcher-uri",
                args.responsible_researcher_uri,
                "--application-domain-uri",
                args.application_domain_uri,
                "--cluster-label",
                args.cluster_label,
                "--cluster-slug",
                args.cluster_slug,
                "--language",
                language,
            ]
            if args.trace_types:
                cmd += ["--trace-types", args.trace_types]
            if args.trace_data_format:
                cmd += ["--trace-data-format", args.trace_data_format]
            if args.electricitymap_token:
                cmd += [
                    "--electricitymap-token", args.electricitymap_token,
                    "--electricitymap-zone", args.electricitymap_zone,
                ]
            print(f"selected_run_id={run_id}")
            print(f"output_file={output_path}")
            subprocess.run(cmd, check=True, env=env)
            generated_files.append(output_path)
        manifest_path = write_manifest(output_dir, generated_files)
        print(f"manifest_file={manifest_path}")
        return

    run_id = args.run_id or latest_finished_run_id(args.airflow_namespace, scheduler_pod, args.dag_id)
    output_path = timestamped_output_path(Path(args.output_dir), TIMESTAMPED_OUTPUT_STEM, args.output_stamp)
    cmd = [
        sys.executable,
        args.collector_path,
        "--airflow-namespace",
        args.airflow_namespace,
        "--namespace",
        args.namespace,
        "--dag-id",
        args.dag_id,
        "--run-id",
        run_id,
        "--exclude-task-ids",
        args.exclude_task_ids,
        "--workflow-name",
        args.workflow_name,
        "--workflow-engine",
        args.workflow_engine,
        "--code-name",
        args.code_name,
        "--code-path",
        args.code_path,
        "--code-uri",
        args.code_uri,
        "--workflow-ui-base-url",
        args.workflow_ui_base_url,
        "--output-file",
        str(output_path),
        "--software-title",
        args.software_title,
        "--publication-uri",
        args.publication_uri,
        "--prom-url",
        args.prom_url,
        "--trace-archive",
        args.trace_archive,
        "--responsible-researcher-uri",
        args.responsible_researcher_uri,
        "--application-domain-uri",
        args.application_domain_uri,
        "--cluster-label",
        args.cluster_label,
        "--cluster-slug",
        args.cluster_slug,
        "--language",
        language,
    ]
    if args.trace_types:
        cmd += ["--trace-types", args.trace_types]
    if args.trace_data_format:
        cmd += ["--trace-data-format", args.trace_data_format]
    if args.electricitymap_token:
        cmd += [
            "--electricitymap-token", args.electricitymap_token,
            "--electricitymap-zone", args.electricitymap_zone,
        ]

    print(f"scheduler_pod={scheduler_pod}")
    print(f"selected_run_id={run_id}")
    print(f"output_file={output_path}")
    subprocess.run(cmd, check=True, env=env)
    manifest_path = write_manifest(output_dir, [output_path])
    print(f"manifest_file={manifest_path}")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        if exc.stdout:
            print(exc.stdout, end="")
        if exc.stderr:
            print(exc.stderr, end="", file=sys.stderr)
        sys.exit(exc.returncode)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
