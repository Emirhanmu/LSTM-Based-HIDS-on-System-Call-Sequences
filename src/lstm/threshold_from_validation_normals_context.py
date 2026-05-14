from pathlib import Path
import pandas as pd


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


def evaluate_threshold(df: pd.DataFrame, threshold: float) -> dict:
    y_pred = (df["trace_score"] >= threshold).astype(int)
    return compute_metrics(df["trace_label"], y_pred)


if __name__ == "__main__":
    base_dir = Path("data") / "processed" / "php_cwe_434"

    val_file = base_dir / "trace_level_context_validation_scores.csv"
    test_file = base_dir / "trace_level_context_test_scores.csv"
    output_file = base_dir / "trace_level_context_validation_normal_thresholds.csv"

    val_df = pd.read_csv(val_file, sep=";")
    test_df = pd.read_csv(test_file, sep=";")

    print("\n====================")
    print("VALIDATION LABEL COUNTS")
    print("====================")
    print(val_df["trace_label"].value_counts(dropna=False))

    print("\n====================")
    print("TEST LABEL COUNTS")
    print("====================")
    print(test_df["trace_label"].value_counts(dropna=False))

    val_normal = val_df[val_df["trace_label"] == 0]["trace_score"]

    print("\n====================")
    print("VALIDATION NORMAL SCORE STATS")
    print("====================")
    print(f"count      = {len(val_normal)}")
    print(f"mean       = {val_normal.mean():.6f}")
    print(f"std        = {val_normal.std():.6f}")
    print(f"q90        = {val_normal.quantile(0.90):.6f}")
    print(f"q95        = {val_normal.quantile(0.95):.6f}")
    print(f"q99        = {val_normal.quantile(0.99):.6f}")
    print(f"max        = {val_normal.max():.6f}")
    print(f"mean+3std  = {(val_normal.mean() + 3 * val_normal.std()):.6f}")

    strategies = {
        "val_normal_q90": float(val_normal.quantile(0.90)),
        "val_normal_q95": float(val_normal.quantile(0.95)),
        "val_normal_q99": float(val_normal.quantile(0.99)),
        "val_normal_max": float(val_normal.max()),
        "val_normal_mean_plus_3std": float(val_normal.mean() + 3 * val_normal.std()),
    }

    rows = []

    print("\n====================")
    print("TEST RESULTS WITH VALIDATION-NORMAL THRESHOLDS")
    print("====================")

    for name, threshold in strategies.items():
        test_metrics = evaluate_threshold(test_df, threshold)

        row = {
            "strategy": name,
            "threshold": threshold,
            "test_precision": test_metrics["precision"],
            "test_recall": test_metrics["recall"],
            "test_f1": test_metrics["f1"],
            "test_fpr": test_metrics["fpr"],
            "test_accuracy": test_metrics["accuracy"],
            "test_tp": test_metrics["tp"],
            "test_tn": test_metrics["tn"],
            "test_fp": test_metrics["fp"],
            "test_fn": test_metrics["fn"],
        }
        rows.append(row)

        print(f"\n{name} (threshold={threshold:.6f})")
        for k, v in test_metrics.items():
            print(f"{k}: {v}")

    result_df = pd.DataFrame(rows).sort_values("test_f1", ascending=False)
    result_df.to_csv(output_file, index=False, sep=";")

    print(f"\nSaved results to: {output_file}")
    print("\nTop strategies by test_f1:")
    print(result_df)