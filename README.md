# FONDA workflow metadata collector

These scripts collect metadata for a finished FORCE / Airflow workflow run and write a Turtle (`.ttl`) file you can upload to VIVO. The output describes the workflow, its execution runs, per-stage metrics (CPU, memory, energy, carbon), the trace archive, the responsible researcher, and the application domain.

## The two scripts

`collect_force1_workflow_metadata_claud_working.py` is the one you run. It finds the finished run(s) of the `force-1` DAG and writes one timestamped `.ttl` per run. It already carries sensible defaults (workflow name, code link, trace archive, trace types, trace data format, responsible researcher, application domain), so a plain run produces a complete file.

`collect_airflow_workflow_metadata_vasilis_claud_working.py` is the collector that does the actual metric gathering and TTL writing. The first script calls it for you. You normally do not run this one directly.

## What you need

1. Python 3.9 or newer.
2. `kubectl` and WireGuard installed on your computer.
3. The cluster access files `config.yml` and `wireguard.conf` (you should have these in Downloads).
4. Both Python files plus `collect_public_metadata_vasilis.py` in the same folder (the collector imports helpers from it).

## Cluster credentials (not in this repo)

`config.yml` (kubeconfig) and `wg0.conf` (WireGuard) are secrets and are deliberately kept out of this repository. Get the real files from the FONDA admin. This repo ships `config.yml.example` and `wg0.conf.example` as reference only. The real files are covered by `.gitignore`, so keep those exact names and they will never be committed.

## Connect to the FONDA cluster

You reach the cluster over a WireGuard VPN, then point `kubectl` at the cluster config.

Step 1. Open a terminal and create a folder for the cluster files:

```
mkdir fondawork
cd fondawork
```

Step 2. Copy the two access files from Downloads into it:

```
cp ../Downloads/config.yml .
cp ../Downloads/wireguard.conf .
```

Step 3. Rename the WireGuard file and bring the VPN up. The rename avoids `wg-quick` picking up a wrong config from `/etc/wireguard/`:

```
mv wireguard.conf wg0.conf
sudo wg-quick up $(pwd)/wg0.conf
```

Step 4. Tell `kubectl` which config to use. Use the full path so it works in every terminal:

```
export KUBECONFIG=$(pwd)/config.yml
```

Step 5. Confirm you are connected:

```
kubectl get nodes
```

If you see the cluster nodes listed, you are in. The VPN stays up for the whole session. When you finish, take it down with `sudo wg-quick down $(pwd)/wg0.conf`.

## Start the port-forwards

Keep these running in their own terminals while you collect. Each terminal must have `KUBECONFIG` exported (repeat Step 4 there, or reuse the connected terminal).

Prometheus (needed by the collector for CPU, memory, energy, carbon):

```
kubectl -n monitoring port-forward svc/prometheus-kube-prometheus-prometheus 9090:9090
```

Airflow web UI (optional, to watch the workflow stages run and confirm they succeed):

```
kubectl -n airflow-yagmur port-forward svc/airflow-yagmur-webserver 8080:8080
```

With the Airflow port-forward running, open `http://127.0.0.1:8080` in your browser to see the workflow's stages and whether the run finished successfully. Collect metadata only after the run is finished. The collector stops early with a clear message if Prometheus is not reachable.

## Run it

Step 1. Open a terminal in the folder that holds these scripts. Make sure the VPN is up and `KUBECONFIG` is exported in this terminal (repeat Step 4 of the connect section if needed).

Step 2. Make sure the Prometheus port-forward is running, and that the workflow run you want has finished (check the Airflow UI).

Step 3. Collect the latest finished run:

```
python3 collect_force1_workflow_metadata_claud_working.py
```

Or collect every finished run of the DAG, one file per run:

```
python3 collect_force1_workflow_metadata_claud_working.py --all-runs
```

Step 4. Find the output. Files are written to the `claudttl/` subfolder, named like:

```
long-term-vegetation-dynamics-mediterranean-workflow-public-metadata-06052026_1055.ttl
```

The timestamp in the name is the run's start time (Berlin time). A `VIVO_UPLOAD_FILES.txt` manifest lists everything that was written.

Step 5. Upload to VIVO. In the VIVO admin, go to Site Admin, then Add/Remove RDF Data, choose format Turtle, pick Add, and upload the `.ttl` file (or paste its contents). Reload the workflow page to see the result.

## Common options

`--all-runs` writes one TTL per finished run instead of only the latest.

`--run-id <id>` collects one specific run.

`--output-dir <path>` changes where files are written (default `claudttl/`).

`--output-stamp 06052026_1055` forces the timestamp used in the filename.

`--trace-types "..."` and `--trace-data-format "..."` override the trace description text.

`--trace-archive <url>` sets the trace archive link.

`--application-domain-uri <uri>` links the workflow to a different Application domain individual.

`--responsible-researcher-uri <uri>` links a different researcher.

`--prom-url http://127.0.0.1:9090` points at a different Prometheus.

`--dag-id force-1` selects a different DAG.

## What the output contains

Each file has three main parts:

1. The workflow individual, with name, code link, UI link, trace archive, trace types, trace data format, responsible researcher, and application domain.
2. The run (execution metadata), with status, engine, language, cluster, timing, CPU, memory, energy, and carbon.
3. One block per workflow stage (preprocessing, mosaic, pyramid, time-series, overhead) with its own metrics.

## If something goes wrong

Prometheus not reachable: the port-forward is not running or points at the wrong service. Restart it and try again.

No finished run found: the DAG has not completed a run yet, or `--dag-id` is wrong.

No task pods found: the run is too old and its Airflow logs were rotated away, so pod names cannot be recovered. Collect soon after a run finishes.
