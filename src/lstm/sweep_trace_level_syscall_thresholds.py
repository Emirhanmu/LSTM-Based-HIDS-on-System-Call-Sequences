from pathlib import Path
import numpy as np
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


def select_threshold_with_constraint(
    validation_df: pd.DataFrame,
    max_fpr_constraint: float | None,
    num_thresholds: int = 500,
):
    min_score = float(validation_df["trace_score"].min())
    max_score = float(validation_df["trace_score"].max())
    thresholds = np.linspace(min_score, max_score, num_thresholds)

    best_threshold = None
    best_metrics = None

    for threshold in thresholds:
        y_pred = (validation_df["trace_score"] >= threshold).astype(int)
        metrics = compute_metrics(validation_df["trace_label"], y_pred)

        if max_fpr_constraint is not None and metrics["fpr"] > max_fpr_constraint:
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

    # Eğer constraint yüzünden hiç threshold kalmazsa, fallback: en iyi F1
    if best_metrics is None:
        for threshold in thresholds:
            y_pred = (validation_df["trace_score"] >= threshold).astype(int)
            metrics = compute_metrics(validation_df["trace_label"], y_pred)

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


def evaluate_with_threshold(df: pd.DataFrame, threshold: float):
    y_pred = (df["trace_score"] >= threshold).astype(int)
    return compute_metrics(df["trace_label"], y_pred)


if __name__ == "__main__":
    base_dir = Path("data") / "processed" / "php_cwe_434"

    validation_file = base_dir / "trace_level_syscall_validation_scores.csv"
    test_file = base_dir / "trace_level_syscall_test_scores.csv"
    output_file = base_dir / "trace_level_syscall_threshold_sweep.csv"

    validation_df = pd.read_csv(validation_file, sep=";")
    test_df = pd.read_csv(test_file, sep=";")

    print("\n====================")
    print("VALIDATION LABEL COUNTS")
    print("====================")
    print(validation_df["trace_label"].value_counts(dropna=False))

    print("\n====================")
    print("TEST LABEL COUNTS")
    print("====================")
    print(test_df["trace_label"].value_counts(dropna=False))

    constraints = [None, 0.40, 0.30, 0.20, 0.10, 0.05]
    rows = []

    for constraint in constraints:
        threshold, val_metrics = select_threshold_with_constraint(
            validation_df=validation_df,
            max_fpr_constraint=constraint,
            num_thresholds=500,
        )

        test_metrics = evaluate_with_threshold(test_df, threshold)

        row = {
            "max_fpr_constraint": "None" if constraint is None else constraint,
            "threshold": threshold,
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
        }
        rows.append(row)

        print("\n====================")
        print(f"Constraint: {row['max_fpr_constraint']}")
        print("====================")
        print(f"Threshold: {threshold:.6f}")
        print(
            f"VAL  -> F1: {val_metrics['f1']:.4f}, "
            f"Recall: {val_metrics['recall']:.4f}, "
            f"Precision: {val_metrics['precision']:.4f}, "
            f"FPR: {val_metrics['fpr']:.4f}"
        )
        print(
            f"TEST -> F1: {test_metrics['f1']:.4f}, "
            f"Recall: {test_metrics['recall']:.4f}, "
            f"Precision: {test_metrics['precision']:.4f}, "
            f"FPR: {test_metrics['fpr']:.4f}, "
            f"Acc: {test_metrics['accuracy']:.4f}"
        )

    result_df = pd.DataFrame(rows)
    result_df.to_csv(output_file, index=False, sep=";")

    print(f"\nSaved threshold sweep results to: {output_file}")
    print("\nTop results by test_f1:")
    print(result_df.sort_values("test_f1", ascending=False).head(10))