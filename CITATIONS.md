# Citation

## Cite this helper repository

Use this repository citation only for the VIVO metadata collection scripts,
the input-dataset descriptions, and the documentation.

```text
Kati, Y. (2026). Long-term Vegetation Dynamics (Airflow) VIVO Metadata. GitHub.
https://github.com/YagmurKati/long-term-vegetation-airflow-vivo-metadata
```

## Cite the workflow

For the FORCE-on-Airflow workflow, workflow execution, and scientific method,
cite the upstream workflow repository:

```text
CRC-FONDA. FONDA Airflow DAGs.
https://github.com/CRC-FONDA/fonda-airflow-dags
```

The related publication that used this workflow is:

```text
A Qualitative Assessment of Using ChatGPT as Large Language Model for
Scientific Workflow Development. GigaScience.
https://doi.org/10.1093/gigascience/giae030
```

## Cite the underlying software

The workflow processes Earth observation data with FORCE. If the FORCE
processing matters to your work, cite:

```text
Frantz, D. (2019). FORCE—Landsat + Sentinel-2 Analysis Ready Data and Beyond.
Remote Sensing, 11(9), 1124. https://doi.org/10.3390/rs11091124
```

## Cite the input data

The runs consume FORCE Level-1/Level-2 products derived from public Landsat
data provided by the USGS. See `input_datasets.json` and the input-dataset
pages it generates for the exact selection, storage locations, and access
statements, and cite the data provider accordingly.

## Scope of this repository

This repository does not redistribute the Airflow DAG or the FORCE software.
It provides scripts and documentation that collect metadata *after* a run has
finished and export it for VIVO. Authorship of the workflow and of its
scientific results rests with the upstream authors.
