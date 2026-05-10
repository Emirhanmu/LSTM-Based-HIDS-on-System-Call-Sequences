import math
from pathlib import Path

import numpy as np
import pandas as pd

from src.baseline.stide import build_stide_memory, score_dataset, compute_metrics


def aggregate_trace_scores(window_df: pd.DataFrame, top_fraction: float = 0.10) -> pd.DataFrame:
    rows = []

    for trace_name, group in window_df.groupby("trace_name"):
        scores = group["score"].sort_values(ascending=False).to_numpy()
        k = max(1, math.ceil(len(scores) * top_fraction))
        trace_score = float(scores[:k].mean())

        trace_label = int(group["label"].max())

        rows.append({
            "trace_name": trace_name,
            "trace_label": trace_label,
            "num_windows": len(group),
            "top_k_used": k,
            "trace_score": trace_score,
        })

    return pd.DataFrame(rows).sort_values("trace_name").reset_index(drop=True)


def select_threshold(
    validation_trace_df: pd.DataFrame,
    max_fpr_constraint: float = 0.20,
    num_thresholds: int = 300,
):
    min_score = float(validation_trace_df["trace_score"].min())
    max_score = float(validation_trace_df["trace_score"].max())
    thresholds = np.linspace(min_score, max_score, num_thresholds)

    best_threshold = None
    best_metrics = None

    for threshold in thresholds:
        y_pred = (validation_trace_df["trace_score"] >= threshold).astype(int)
        metrics = compute_metrics(validation_trace_df["trace_label"], y_pred)

        if metrics["fpr"] > max_fpr_constraint:
            continue

        if best_metrics is None:
            best_threshold = float(threshold)
            best_metrics = metrics
        else:
            current_key = (metrics["f1"], metrics["recall"], metrics["precision"])
            best_key = (best_metrics["f1"], best_metrics["recall"], best_metrics["precision"])
            if current_key > best_key:
                best_threshold = float(threshold)
                best_metrics = metrics

    if best_metrics is None:
        for threshold in thresholds:
            y_pred = (validation_trace_df["trace_score"] >= threshold).astype(int)
            metrics = compute_metrics(validation_trace_df["trace_label"], y_pred)

            if best_metrics is None:
                best_threshold = float(threshold)
                best_metrics = metrics
            else:
                current_key = (metrics["f1"], metrics["recall"], metrics["precision"])
                best_key = (best_metrics["f1"], best_metrics["recall"], best_metrics["precision"])
                if current_key > best_key:
                    best_threshold = float(threshold)
                    best_metrics = metrics

    return best_threshold, best_metrics


def evaluate_trace_df(trace_df: pd.DataFrame, threshold: float) -> dict:
    y_pred = (trace_df["trace_score"] >= threshold).astype(int)
    return compute_metrics(trace_df["trace_label"], y_pred)


if __name__ == "__main__":
    scenario = "php_cwe_434"

    train_csv = r"data\processed\php_cwe_434\train_windows.csv"
    validation_csv = r"data\processed\php_cwe_434\validation_windows.csv"
    test_csv = r"data\processed\php_cwe_434\test_windows.csv"

    output_val = r"data\processed\php_cwe_434\trace_level_stide_validation_scores.csv"
    output_test = r"data\processed\php_cwe_434\trace_level_stide_test_scores.csv"
    output_summary = r"data\processed\php_cwe_434\trace_level_stide_eval_summary.csv"

    ngram_size = 6
    top_fraction = 0.10
    max_train_rows = 50_000

    print("\nBuilding STIDE memory...")
    stide_memory = build_stide_memory(
        train_csv=train_csv,
        ngram_size=ngram_size,
        max_rows=max_train_rows,
        chunksize=10_000,
    )

    print("\nScoring full validation windows...")
    validation_window_df = score_dataset(
        csv_path=validation_csv,
        memory=stide_memory,
        ngram_size=ngram_size,
        max_rows=None,
        chunksize=10_000,
    )

    print("\nScoring full test windows...")
    test_window_df = score_dataset(
        csv_path=test_csv,
        memory=stide_memory,
        ngram_size=ngram_size,
        max_rows=None,
        chunksize=10_000,
    )

    validation_trace_df = aggregate_trace_scores(validation_window_df, top_fraction=top_fraction)
    test_trace_df = aggregate_trace_scores(test_window_df, top_fraction=top_fraction)

    Path(output_val).parent.mkdir(parents=True, exist_ok=True)
    validation_trace_df.to_csv(output_val, index=False, sep=";")
    test_trace_df.to_csv(output_test, index=False, sep=";")

    print("\n====================")
    print("TRACE-LEVEL LABEL COUNTS")
    print("====================")
    print("Validation:")
    print(validation_trace_df["trace_label"].value_counts(dropna=False))
    print("\nTest:")
    print(test_trace_df["trace_label"].value_counts(dropna=False))

    best_threshold, val_metrics = select_threshold(
        validation_trace_df,
        max_fpr_constraint=0.20,
        num_thresholds=300,
    )

    test_metrics = evaluate_trace_df(test_trace_df, best_threshold)

    print("\n====================")
    print("TRACE-LEVEL STIDE RESULTS")
    print("====================")
    print(f"N-gram size: {ngram_size}")
    print(f"Top fraction: {top_fraction}")
    print(f"Best threshold: {best_threshold:.6f}")

    print("\nValidation metrics:")
    for k, v in val_metrics.items():
        print(f"{k}: {v}")

    print("\nTest metrics:")
    for k, v in test_metrics.items():
        print(f"{k}: {v}")

    summary_df = pd.DataFrame([{
        "method": "STIDE_TRACE_LEVEL",
        "ngram_size": ngram_size,
        "top_fraction": top_fraction,
        "threshold": best_threshold,
        "val_precision": val_metrics["precision"],
        "val_recall": val_metrics["recall"],
        "val_f1": val_metrics["f1"],
        "val_fpr": val_metrics["fpr"],
        "test_precision": test_metrics["precision"],
        "test_recall": test_metrics["recall"],
        "test_f1": test_metrics["f1"],
        "test_fpr": test_metrics["fpr"],
        "test_accuracy": test_metrics["accuracy"],
        "test_tp": test_metrics["tp"],
        "test_tn": test_metrics["tn"],
        "test_fp": test_metrics["fp"],
        "test_fn": test_metrics["fn"],
    }])

    summary_df.to_csv(output_summary, index=False, sep=";")
    print(f"\nSaved validation trace scores to: {output_val}")
    print(f"Saved test trace scores to: {output_test}")
    print(f"Saved summary to: {output_summary}")