from pathlib import Path
import math
import time
import heapq
from collections import defaultdict

import numpy as np
import pandas as pd


def generate_ngrams(tokens, n=6):
    if len(tokens) < n:
        return []
    return [tuple(tokens[i:i+n]) for i in range(len(tokens) - n + 1)]


def stide_score(window_syscalls: str, memory_set: set, ngram_size: int = 6) -> float:
    tokens = str(window_syscalls).strip().split()
    ngrams = generate_ngrams(tokens, n=ngram_size)

    if not ngrams:
        return 0.0

    mismatches = sum(1 for ng in ngrams if ng not in memory_set)
    return mismatches / len(ngrams)


def compute_metrics(y_true: pd.Series, y_pred: pd.Series) -> dict:
    tp = int(((y_true == 1) & (y_pred == 1)).sum())
    tn = int(((y_true == 0) & (y_pred == 0)).sum())
    fp = int(((y_true == 0) & (y_pred == 1)).sum())
    fn = int(((y_true == 1) & (y_pred == 0)).sum())

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0
    accuracy = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0 else 0.0

    return {
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "fpr": fpr,
        "accuracy": accuracy,
    }


def aggregate_trace_scores_full(window_df: pd.DataFrame, top_fraction: float = 0.10) -> pd.DataFrame:
    rows = []

    for trace_name, group in window_df.groupby("trace_name"):
        scores = group["window_score"].sort_values(ascending=False).to_numpy()
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


def select_threshold(validation_trace_df: pd.DataFrame, max_fpr_constraint: float = 0.20, num_thresholds: int = 300):
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


def build_stide_memory(train_csv: str, ngram_size: int = 6, max_rows: int = 500_000, chunksize: int = 100_000):
    print("\n====================")
    print("BUILDING STIDE MEMORY")
    print("====================")

    memory_set = set()
    rows_seen = 0
    start = time.time()

    for chunk in pd.read_csv(train_csv, sep=";", usecols=["window_syscalls"], chunksize=chunksize):
        for seq in chunk["window_syscalls"]:
            tokens = str(seq).strip().split()
            ngrams = generate_ngrams(tokens, n=ngram_size)
            memory_set.update(ngrams)

        rows_seen += len(chunk)

        print(f"Processed train windows: {rows_seen:,} | Unique {ngram_size}-grams: {len(memory_set):,}")

        if max_rows is not None and rows_seen >= max_rows:
            break

    elapsed = time.time() - start
    print(f"\nFinal train windows used: {min(rows_seen, max_rows) if max_rows is not None else rows_seen:,}")
    print(f"Final unique {ngram_size}-grams in memory: {len(memory_set):,}")
    print(f"Memory build elapsed: {elapsed/60:.1f} min")

    return memory_set


def build_validation_trace_df(validation_csv: str, memory_set: set, ngram_size: int = 6, chunksize: int = 100_000):
    print("\nScoring validation set...")
    rows = []
    seen = 0
    start = time.time()

    for chunk in pd.read_csv(validation_csv, sep=";", usecols=["trace_name", "label", "window_syscalls"], chunksize=chunksize):
        scores = [stide_score(seq, memory_set, ngram_size=ngram_size) for seq in chunk["window_syscalls"]]

        chunk_rows = pd.DataFrame({
            "trace_name": chunk["trace_name"].values,
            "label": chunk["label"].values,
            "window_score": scores,
        })
        rows.append(chunk_rows)

        seen += len(chunk)
        if seen % 1_000_000 == 0:
            elapsed = time.time() - start
            print(f"Validation processed windows: {seen:,} | elapsed: {elapsed/60:.1f} min")

    window_df = pd.concat(rows, ignore_index=True)
    return aggregate_trace_scores_full(window_df, top_fraction=0.10)


def count_test_windows_per_trace(test_csv, chunksize=200_000):
    print("\nCounting test windows per trace...")
    counts = defaultdict(int)
    labels = {}
    total_rows = 0
    start = time.time()

    for chunk_idx, chunk in enumerate(
        pd.read_csv(test_csv, sep=";", usecols=["trace_name", "label"], chunksize=chunksize),
        start=1
    ):
        vc = chunk["trace_name"].value_counts()
        for trace_name, cnt in vc.items():
            counts[trace_name] += int(cnt)

        label_map = chunk.groupby("trace_name")["label"].max().to_dict()
        labels.update(label_map)

        total_rows += len(chunk)

        if chunk_idx % 20 == 0:
            elapsed = time.time() - start
            print(f"Count pass rows: {total_rows:,} | traces: {len(counts):,} | elapsed: {elapsed/60:.1f} min")

    print(f"Finished count pass. Total rows: {total_rows:,}, total traces: {len(counts):,}")
    return counts, labels


def streaming_test_trace_df(test_csv: str, memory_set: set, ngram_size: int = 6, top_fraction: float = 0.10, chunksize: int = 50_000):
    counts, labels = count_test_windows_per_trace(test_csv, chunksize=200_000)

    k_map = {
        trace_name: max(1, math.ceil(cnt * top_fraction))
        for trace_name, cnt in counts.items()
    }

    heaps = {trace_name: [] for trace_name in counts.keys()}

    total_rows = 0
    start = time.time()

    print("\nScoring test set with streaming top-k aggregation...")

    total_expected = sum(counts.values())

    for chunk_idx, chunk in enumerate(
        pd.read_csv(test_csv, sep=";", usecols=["trace_name", "label", "window_syscalls"], chunksize=chunksize),
        start=1
    ):
        scores = [stide_score(seq, memory_set, ngram_size=ngram_size) for seq in chunk["window_syscalls"]]

        for trace_name, score in zip(chunk["trace_name"].values, scores):
            heap = heaps[trace_name]
            k = k_map[trace_name]

            if len(heap) < k:
                heapq.heappush(heap, score)
            else:
                if score > heap[0]:
                    heapq.heapreplace(heap, score)

        total_rows += len(chunk)

        if chunk_idx % 20 == 0:
            elapsed = time.time() - start
            rate = total_rows / max(elapsed, 1e-9)
            remaining = total_expected - total_rows
            eta_min = (remaining / rate) / 60 if rate > 0 else -1
            print(
                f"Test processed rows: {total_rows:,} / {total_expected:,} "
                f"| traces: {len(heaps):,} | elapsed: {elapsed/60:.1f} min | ETA: {eta_min:.1f} min"
            )

    rows = []
    for trace_name, heap in heaps.items():
        if not heap:
            continue
        trace_score = float(np.mean(heap))
        rows.append({
            "trace_name": trace_name,
            "trace_label": int(labels[trace_name]),
            "num_windows": counts[trace_name],
            "top_k_used": k_map[trace_name],
            "trace_score": trace_score,
        })

    trace_df = pd.DataFrame(rows).sort_values("trace_name").reset_index(drop=True)
    return trace_df


if __name__ == "__main__":
    base_dir = Path("data") / "processed" / "php_cwe_434"

    train_csv = base_dir / "train_windows.csv"
    validation_csv = base_dir / "validation_windows.csv"
    test_csv = base_dir / "test_windows.csv"

    output_val = base_dir / "trace_level_stide_validation_scores.csv"
    output_test = base_dir / "trace_level_stide_test_scores.csv"
    output_summary = base_dir / "trace_level_stide_eval_summary.csv"

    ngram_size = 6

    memory_set = build_stide_memory(
        train_csv=str(train_csv),
        ngram_size=ngram_size,
        max_rows=500_000,
        chunksize=100_000,
    )

    validation_trace_df = build_validation_trace_df(
        validation_csv=str(validation_csv),
        memory_set=memory_set,
        ngram_size=ngram_size,
        chunksize=100_000,
    )

    test_trace_df = streaming_test_trace_df(
        test_csv=str(test_csv),
        memory_set=memory_set,
        ngram_size=ngram_size,
        top_fraction=0.10,
        chunksize=50_000,
    )

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
        "top_fraction": 0.10,
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