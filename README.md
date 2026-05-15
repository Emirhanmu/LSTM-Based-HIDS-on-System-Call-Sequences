
# LSTM-Based Anomaly Detection on Linux System Call Sequences

This repository contains our capstone project on host-based anomaly detection in Linux systems using system call sequences. The project studies whether anomalous process behavior can be detected by learning normal execution patterns from Linux system calls.

## Research Question

Can anomalous Linux process behavior be reliably detected from normal patterns learned from system call sequences?

## Project Scope

This study focuses on a **single-scenario case study** using the **LID-DS 2021** dataset, specifically the **PHP_CWE-434** scenario. We compare a classical sequence-based baseline (**STIDE**) with learned sequence models (**LSTM syscall-only** and **LSTM context-aware**).

The project is designed as an **offline trace-level anomaly detection prototype**, not a real-time production IDS.

---

## Dataset

- **Dataset:** LID-DS 2021
- **Scenario used in this repository:** `PHP_CWE-434`
- **Data type:** Linux system call traces
- **Core fields used:**
  - `syscall_name`
  - `process_name`
  - `return_status`
  - `thread_id`

### Important Notes
- Training is performed on **normal traces only**
- Evaluation is performed on **unseen normal and attack-containing traces**
- Splits are **trace-disjoint**
- No trace-name overlap exists across train / validation / test

---

## Methodology

### 1. Parsing and preprocessing
Raw `.sc` traces are parsed into structured records. We extract the relevant fields and clean malformed rows before sequence construction.

### 2. Thread-based sequence generation
System calls are grouped by `thread_id`, then converted into fixed-length windows using a sliding window approach.

- **Window size:** 30
- **Stride:** 1

### 3. Models
We evaluate three methods:

#### STIDE
A classical n-gram based baseline using syscall sequence mismatch scores.

#### LSTM Syscall
An LSTM next-token prediction model trained only on encoded syscall sequences.

#### LSTM Context
An LSTM next-token prediction model trained on encoded context sequences that include syscall-related contextual signals.

### 4. Trace-level decision logic
Each window receives an anomaly score.  
Window scores are then aggregated into a single **trace-level score** using:

- **Top-10% mean pooling**

This means the final trace score is the average of the top 10% most anomalous windows in the trace.

### 5. Thresholding
Our final reporting uses:

- **Primary operating point:** validation-normal-based `q95`
- **Sensitivity analysis:** validation-normal-based `q90`
- **Original anomaly-based thresholding:** reported separately for completeness

---

## Main Results

### ROC-AUC
| Model | AUC-ROC |
|---|---:|
| STIDE | 0.9947 |
| LSTM Syscall | 0.9962 |
| LSTM Context | 0.9915 |

### Main Comparison (validation-normal q95)
| Model | Precision | Recall | F1 | FPR |
|---|---:|---:|---:|---:|
| STIDE | 0.9412 | 0.8421 | 0.8889 | 0.0079 |
| LSTM Syscall | 0.9469 | 0.9386 | 0.9427 | 0.0079 |
| LSTM Context | 0.8976 | 1.0000 | 0.9461 | 0.0171 |

### Interpretation
- **STIDE** is a strong classical baseline
- **LSTM Syscall** gives the best overall balance between precision, recall, and false positive rate
- **LSTM Context** achieves the highest recall, but with a higher false positive rate

### Sensitivity (validation-normal q90)
| Model | Precision | Recall | F1 | FPR |
|---|---:|---:|---:|---:|
| STIDE | 0.9194 | 1.0000 | 0.9580 | 0.0131 |
| LSTM Syscall | 0.9421 | 1.0000 | 0.9702 | 0.0092 |
| LSTM Context | 0.8769 | 1.0000 | 0.9344 | 0.0210 |

---

## Repository Structure

```text
src/
  baseline/
    trace_level_stide_eval_fast.py
    threshold_from_validation_normals_stide.py

  features/
    encode_sequences.py

  lstm/
    trace_level_syscall_eval_fast.py
    trace_level_context_eval_fast.py
    threshold_from_validation_normals.py
    threshold_from_validation_normals_context.py
    sweep_trace_level_syscall_thresholds.py

  pipeline/
    build_datasets.py
    check_split_leakage.py
    check_attack_trace_split.py

  reporting/
    build_final_results_tables.py
    make_final_plots.py
    bootstrap_q95_metrics.py
    error_analysis_q95.py
    plot_lstm_syscall_profiles.py

  demo/
    demo_app.py
    demo_utils.py
    explanation_utils.py
    build_stide_artifact.py

results/
  final_tables/
  final_figures/
  bootstrap/
  error_analysis/
  profiles/

data/
  artifacts/
  processed/php_cwe_434/

```


## Interactive Demo Dashboard

This repository also includes a local Streamlit-based demo for trace-level inference.

### Demo capabilities
The dashboard can:

- Upload a **raw trace** as `.zip` or `.sc`
- Upload a **prepared single-trace windows CSV**
- Run **one or more models** on the same trace:
  - STIDE
  - LSTM Syscall
  - LSTM Context
- Let the user choose the threshold policy:
  - `q95`
  - `q90`
  - `original`
- Display:
  - trace-level comparison table
  - model-specific decision cards
  - window score profile plot
  - top suspicious windows
  - pattern-level explanation for STIDE and LSTM Syscall
  - context-aware explanation for LSTM Context

### Important note
The dashboard is an **offline inference demo**.  
It does **not** retrain models and does **not** represent a real-time production deployment.

### Required artifacts
Before running the dashboard, the following artifacts must already exist:

- `data/processed/php_cwe_434/lstm_sequence_predictor.pt`
- `data/processed/php_cwe_434/lstm_context_sequence_predictor.pt`
- `data/processed/php_cwe_434/syscall_vocab.json`
- `data/processed/php_cwe_434/context_vocab.json`

The STIDE artifact must also be built once:

```bash
python -m src.demo.build_stide_artifact

streamlit run .\src\demo\demo_app.py
