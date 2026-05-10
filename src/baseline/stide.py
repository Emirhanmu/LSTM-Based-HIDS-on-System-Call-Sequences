from pathlib import Path
from typing import List, Tuple

import pandas as pd


def parse_window(window_text: str) -> List[str]:
    if pd.isna(window_text):
        return []
    return str(window_text).strip().split()


def generate_ngrams(tokens: List[str], n: int) -> List[Tuple[str, ...]]:
    if len(tokens) < n:
        return []
    return [tuple(tokens[i:i + n]) for i in range(len(tokens) - n + 1)]


def build_stide_memory(
    train_csv: str,
    ngram_size: int = 6,
    max_rows: int | None = 50_000,
    chunksize: int = 10_000,
) -> set:
    memory = set()
    processed = 0

    print("\n====================")
    print("BUILDING STIDE MEMORY")
    print("====================")

    for chunk in pd.read_csv(
        train_csv,
        sep=";",
        usecols=["window_syscalls", "label"],
        chunksize=chunksize,
    ):
        chunk = chunk[chunk["label"] == 0]

        for window_text in chunk["window_syscalls"]:
            tokens = parse_window(window_text)
            grams = generate_ngrams(tokens, ngram_size)

            for gram in grams:
                memory.add(gram)

            processed += 1
            if max_rows is not None and processed >= max_rows:
                break

        print(f"Processed train windows: {processed:,} | Unique {ngram_size}-grams: {len(memory):,}")

        if max_rows is not None and processed >= max_rows:
            break

    print(f"\nFinal train windows used: {processed:,}")
    print(f"Final unique {ngram_size}-grams in memory: {len(memory):,}")

    return memory


def score_window(window_text: str, memory: set, ngram_size: int) -> float:
    tokens = parse_window(window_text)
    grams = generate_ngrams(tokens, ngram_size)

    if not grams:
        return 1.0

    unseen = sum(1 for gram in grams if gram not in memory)
    return unseen / len(grams)


def score_dataset(
    csv_path: str,
    memory: set,
    ngram_size: int = 6,
    max_rows: int | None = 20_000,
    chunksize: int = 10_000,
) -> pd.DataFrame:
    scored_rows = []
    processed = 0

    print(f"\n====================")
    print(f"SCORING DATASET: {csv_path}")
    print(f"====================")

    for chunk in pd.read_csv(
        csv_path,
        sep=";",
        usecols=["trace_name", "label", "window_syscalls"],
        chunksize=chunksize,
    ):
        for _, row in chunk.iterrows():
            score = score_window(row["window_syscalls"], memory, ngram_size)

            scored_rows.append({
                "trace_name": row["trace_name"],
                "label": int(row["label"]),
                "score": score,
            })

            processed += 1
            if max_rows is not None and processed >= max_rows:
                break

        print(f"Processed scored windows: {processed:,}")

        if max_rows is not None and processed >= max_rows:
            break

    df = pd.DataFrame(scored_rows)
    print(f"Scored dataframe shape: {df.shape}")
    return df


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


def select_threshold(validation_df: pd.DataFrame, step: float = 0.01) -> tuple[float, dict]:
    best_threshold = 0.0
    best_metrics = None

    thresholds = [round(i * step, 4) for i in range(int(1 / step) + 1)]

    print("\n====================")
    print("SELECTING THRESHOLD ON VALIDATION")
    print("====================")
    print(validation_df["label"].value_counts(dropna=False))
    
    for threshold in thresholds:
        y_pred = (validation_df["score"] >= threshold).astype(int)
        metrics = compute_metrics(validation_df["label"], y_pred)

        if best_metrics is None:
            best_threshold = threshold
            best_metrics = metrics
            continue

        # Önce F1, eşitse recall, sonra precision
        current_key = (metrics["f1"], metrics["recall"], metrics["precision"])
        best_key = (best_metrics["f1"], best_metrics["recall"], best_metrics["precision"])

        if current_key > best_key:
            best_threshold = threshold
            best_metrics = metrics

    print(f"Best threshold: {best_threshold}")
    print("Best validation metrics:")
    for k, v in best_metrics.items():
        print(f"  {k}: {v}")

    return best_threshold, best_metrics


def evaluate_with_threshold(df: pd.DataFrame, threshold: float, name: str) -> dict:
    y_pred = (df["score"] >= threshold).astype(int)
    metrics = compute_metrics(df["label"], y_pred)

    print(f"\n====================")
    print(f"{name.upper()} EVALUATION")
    print(f"====================")
    print(f"Threshold: {threshold}")
    for k, v in metrics.items():
        print(f"{k}: {v}")

    return metrics


def save_summary(
    output_file: str,
    scenario: str,
    ngram_size: int,
    threshold: float,
    val_metrics: dict,
    test_metrics: dict,
    train_rows_used: int,
    val_rows_used: int,
    test_rows_used: int,
):
    summary_df = pd.DataFrame([{
        "scenario": scenario,
        "method": "STIDE",
        "ngram_size": ngram_size,
        "threshold": threshold,
        "train_rows_used": train_rows_used,
        "validation_rows_used": val_rows_used,
        "test_rows_used": test_rows_used,
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

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(output_file, index=False, sep=";")
    print(f"\nSaved STIDE summary to: {output_file}")


if __name__ == "__main__":
    scenario = "php_cwe_434"

    train_csv = r"data\processed\php_cwe_434\train_windows.csv"
    validation_csv = r"data\processed\php_cwe_434\validation_windows.csv"
    test_csv = r"data\processed\php_cwe_434\test_windows.csv"

    ngram_size = 6

    # İlk deneme için küçük subset
    max_train_rows = 50_000
    max_val_rows = 20_000
    max_test_rows = 20_000

    stide_memory = build_stide_memory(
        train_csv=train_csv,
        ngram_size=ngram_size,
        max_rows=max_train_rows,
        chunksize=10_000,
    )

    validation_scores = score_dataset(
        csv_path=validation_csv,
        memory=stide_memory,
        ngram_size=ngram_size,
        max_rows=max_val_rows,
        chunksize=10_000,
    )

    best_threshold, val_metrics = select_threshold(validation_scores, step=0.01)

    test_scores = score_dataset(
        csv_path=test_csv,
        memory=stide_memory,
        ngram_size=ngram_size,
        max_rows=max_test_rows,
        chunksize=10_000,
    )

    test_metrics = evaluate_with_threshold(test_scores, best_threshold, name="test")

    save_summary(
        output_file=r"data\processed\php_cwe_434\stide_summary.csv",
        scenario=scenario,
        ngram_size=ngram_size,
        threshold=best_threshold,
        val_metrics=val_metrics,
        test_metrics=test_metrics,
        train_rows_used=max_train_rows,
        val_rows_used=max_val_rows,
        test_rows_used=max_test_rows,
    )