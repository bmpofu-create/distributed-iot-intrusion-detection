# Distributed IoT Intrusion Detection and Network Traffic Analytics

PySpark pipeline for the [RT-IoT2022](https://www.kaggle.com/datasets/supplejade/rt-iot2022real-time-internet-of-things) network-flow dataset: clean and encode 123,117 flows, summarize them by attack type, and train a 12-class logistic regression model. The same three modules are timed at 1, 2, and 4 executors.

The full write-up is in [REPORT.md](REPORT.md). The proposal is in [docs/project-proposal.pdf](docs/project-proposal.pdf). Saved metrics and figures from the completed run are in `outputs/`.

GitHub: https://github.com/bmpofu-create/distributed-iot-intrusion-detection

## Team

| Member | Module | PySpark API |
| --- | --- | --- |
| Beven Mpofu | Clean, engineer, encode, scale | `StringIndexer`, `OneHotEncoder`, `VectorAssembler`, `StandardScaler` |
| Memory Mhembere | Group summaries and Pearson correlation | `pyspark.sql.functions`, `Correlation.corr` |
| Sibonginkosi T Nkashe | Multiclass logistic regression, 80/20 split | `LogisticRegression`, `MulticlassClassificationEvaluator` |

## Result in brief

On a 2-CPU, 13.61 GB host, multinomial logistic regression reached **94.6% test accuracy** and **0.94 weighted F1**, against a 76.7% majority-class baseline (`DOS_SYN_Hping`). **Macro F1 is 0.60**: rare classes such as `DDOS_Slowloris`, `Wipro_bulb`, `Metasploit_Brute_Force_SSH`, and `NMAP_FIN_SCAN` are barely detected.

Adding executors did not speed the jobs up. At 4 executors every module was slower than at 1 (speedup 0.40–0.43). The file is about 52 MB, the host has 2 CPUs, and 4 one-core executors oversubscribe that machine. See [REPORT.md](REPORT.md) for the timing table.

## Repository layout

```text
data/RT_IOT2022.csv          dataset (123,117 rows, 85 columns, about 52 MB)
notebooks/spark_iot_project.ipynb
outputs/                     CSV and JSON results from the timed run
outputs/figures/             scaling, correlation, and confusion-matrix plots
docs/project-proposal.pdf
REPORT.md
requirements.txt
```

The notebook used to write into a folder named `ouputs`. That name was a typo. Results now live in `outputs/`. If an empty `ouputs` directory is still on disk, it is leftover from OneDrive and is not part of the project.

## Run the notebook

The cluster scripts are Linux shell scripts. Run the notebook in **Google Colab** or **WSL / Linux** with Java 17. Native Windows PowerShell cannot start the standalone master.

```bash
pip install -r requirements.txt
```

Colab: upload `data/RT_IOT2022.csv` to `/content/RT_IOT2022.csv`, then run all cells.

Local clone: open `notebooks/spark_iot_project.ipynb` from the repository root (or from `notebooks/`). The notebook finds `data/RT_IOT2022.csv` and writes tables to `outputs/` and figures to `outputs/figures/`.

Optional shorter rerun:

```bash
export IOT_EXECUTORS=1
export IOT_REPEATS=1
```

Defaults are executors `1,2,4` and `3` repeats (27 timed jobs). The submitted 2-CPU run took on the order of 45 minutes.

## Dataset

RT-IoT2022, real-time IoT network traffic, 123,117 flows and 85 attributes. No synthetic resampling. Source: [Kaggle, supplejade/rt-iot2022](https://www.kaggle.com/datasets/supplejade/rt-iot2022real-time-internet-of-things).

`data/RT_IOT2022.csv` is about 52 MB. GitHub warns on files over 50 MB and rejects files over 100 MB. If `git push` complains, track that file with Git LFS before the first push:

```bash
git lfs install
git lfs track "data/RT_IOT2022.csv"
git add .gitattributes data/RT_IOT2022.csv
```
