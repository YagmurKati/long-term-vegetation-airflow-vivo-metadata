# FONDA workflow metadata collector

These scripts collect metadata for a finished FORCE / Airflow workflow run and write a Turtle (`.ttl`) file you can upload to VIVO. The output describes the workflow, its execution runs, per-stage metrics (CPU, memory, energy, carbon), the trace archive, the responsible researcher, and the application domain.

## The two scripts

`collect_force1_workflow_metadata_claud_working.py` is the one you run. It finds the finished run(s) of the `force-1` DAG and writes one timestamped `.ttl` per run. It ships with sensible defaults (workflow name, code link, trace archive, trace types, trace data format, responsible researcher, application domain), so a plain run produces a complete file.

`collect_airflow_workflow_metadata_vasilis_claud_working.py` is the collector that gathers the metrics and writes the TTL. The first script calls it for you, so you normally do not run it directly.

## What you need

1. Python 3.9 or newer.
2. `kubectl` and WireGuard installed.
3. Access to the FONDA cluster (see below).
4. All three Python files in the same folder: the two above plus `collect_public_metadata_vasilis.py`, which the collector imports.

## Cluster access

The workflow runs on the FONDA Kubernetes cluster, which you reach over a WireGuard VPN and a kubeconfig. You need two files:

1. `wireguard.conf` — the WireGuard VPN profile.
2. `config.yml` — the kubeconfig, provided by the FONDA admin, Vasilis Bountris.

Keep both in your work folder. They are credentials, so they are not part of this repository: `.gitignore` excludes them, and the repo ships `wg0.conf.example` and `config.yml.example` as structure references only.

## Connect to the cluster

1. Copy the two access files into your work folder.
2. Rename the WireGuard profile and bring the VPN up. The rename keeps `wg-quick` from picking up a different config in `/etc/wireguard/`:

   ```
   mv wireguard.conf wg0.conf
   sudo wg-quick up $(pwd)/wg0.conf
   ```
3. Point `kubectl` at the kubeconfig. Use the full path so it works in every terminal:

   ```
   export KUBECONFIG=$(pwd)/config.yml
   ```
4. Confirm you are connected:

   ```
   kubectl get nodes
   ```

   If the cluster nodes are listed, you are in. The VPN stays up for the session; take it down when you finish with `sudo wg-quick down $(pwd)/wg0.conf`.

## Start the port-forwards

Keep these running in their own terminals while you collect. Each terminal needs `KUBECONFIG` exported (repeat step 3 above, or reuse the connected terminal).

1. Prometheus, required by the collector for CPU, memory, energy, and carbon:

   ```
   kubectl -n monitoring port-forward svc/prometheus-kube-prometheus-prometheus 9090:9090
   ```
2. Airflow web UI, optional, to watch the workflow stages run and confirm they finish:

   ```
   kubectl -n airflow-yagmur port-forward svc/airflow-yagmur-webserver 8080:8080
   ```

   With this running, open `http://127.0.0.1:8080` to follow the workflow's stages. Collect metadata only after the run has finished.

## Run the collector

1. Open a terminal in the folder that holds these scripts, with the VPN up and `KUBECONFIG` exported.
2. Make sure the Prometheus port-forward is running and the run you want has finished.
3. Collect the latest finished run:

   ```
   python3 collect_force1_workflow_metadata_claud_working.py
   ```

   Or collect every finished run of the DAG, one file per run:

   ```
   python3 collect_force1_workflow_metadata_claud_working.py --all-runs
   ```
4. Find the output in the `claudttl/` subfolder, named like `long-term-vegetation-dynamics-mediterranean-workflow-public-metadata-06052026_1055.ttl`. The timestamp is the run's start time in Berlin time. A `VIVO_UPLOAD_FILES.txt` manifest lists everything written.
5. Upload to VIVO: in the VIVO admin, go to Site Admin, then Add/Remove RDF Data, choose format Turtle, select Add, and upload the `.ttl` file. Reload the workflow page to see the result.

The `samples_ttl/` folder contains example output for two runs so you can compare against your own.

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

## Troubleshooting

Prometheus not reachable: the port-forward is not running or points at the wrong service. Restart it and try again.

No finished run found: the DAG has not completed a run yet, or `--dag-id` is wrong.

No task pods found: the run is too old and its Airflow logs were rotated away, so pod names cannot be recovered. Collect soon after a run finishes.
