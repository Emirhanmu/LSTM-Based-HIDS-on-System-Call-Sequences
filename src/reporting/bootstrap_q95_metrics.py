from pathlib import Path
import numpy as np
import pandas as pd


BASE_DIR = Path("data") / "processed" / "php_cwe_434"
OUT_DIR = Path("results") / "bootstrap"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MODEL_FILES = {
    "STIDE": {
        "val": BASE_DIR / "trace_level_stide_validation_scores.csv",
        "test": BASE_DIR / "trace_level_stide_test_scores.csv",
    },
    "LSTM Syscall": {
        "val": BASE_DIR / "trace_level_syscall_validation_scores.csv",
        "test": BASE_DIR / "trace_level_syscall_test_scores.csv",
    },
    "LSTM Context": {
        "val": BASE_DIR / "trace_level_context_validation_scores.csv",
        "test": BASE_DIR / "trace_level_context_test_scores.csv",
    },
}


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
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


def percentile_ci(values, alpha=0.05):
    lower = np.quantile(values, alpha / 2)
    upper = np.quantile(values, 1 - alpha / 2)
    return float(lower), float(upper)


def bootstrap_model(model_name: str, val_file: Path, test_file: Path, n_boot: int = 1000, seed: int = 42):
    rng = np.random.default_rng(seed)

    val_df = pd.read_csv(val_file, sep=";")
    test_df = pd.read_csv(test_file, sep=";")

    val_normal = val_df[val_df["trace_label"] == 0]["trace_score"].to_numpy()
    test_scores = test_df["trace_score"].to_numpy()
    test_labels = test_df["trace_label"].to_numpy()

    n_val = len(val_normal)
    n_test = len(test_df)

    rows = []

    for i in range(n_boot):
        val_sample = rng.choice(val_normal, size=n_val, replace=True)
        threshold = float(np.quantile(val_sample, 0.95))

        test_idx = rng.choice(np.arange(n_test), size=n_test, replace=True)
        boot_scores = test_scores[test_idx]
        boot_labels = test_labels[test_idx]

        y_pred = (boot_scores >= threshold).astype(int)
        metrics = compute_metrics(boot_labels, y_pred)

        rows.append({
            "bootstrap_id": i,
            "threshold": threshold,
            **metrics,
        })

    boot_df = pd.DataFrame(rows)
    boot_df.to_csv(OUT_DIR / f"{model_name.lower().replace(' ', '_')}_q95_bootstrap_samples.csv", index=False)

    summary = {
        "model": model_name,
        "n_boot": n_boot,
        "threshold_mean": boot_df["threshold"].mean(),
        "threshold_ci_low": percentile_ci(boot_df["threshold"])[0],
        "threshold_ci_high": percentile_ci(boot_df["threshold"])[1],

        "precision_mean": boot_df["precision"].mean(),
        "precision_ci_low": percentile_ci(boot_df["precision"])[0],
        "precision_ci_high": percentile_ci(boot_df["precision"])[1],

        "recall_mean": boot_df["recall"].mean(),
        "recall_ci_low": percentile_ci(boot_df["recall"])[0],
        "recall_ci_high": percentile_ci(boot_df["recall"])[1],

        "f1_mean": boot_df["f1"].mean(),
        "f1_ci_low": percentile_ci(boot_df["f1"])[0],
        "f1_ci_high": percentile_ci(boot_df["f1"])[1],

        "fpr_mean": boot_df["fpr"].mean(),
        "fpr_ci_low": percentile_ci(boot_df["fpr"])[0],
        "fpr_ci_high": percentile_ci(boot_df["fpr"])[1],

        "accuracy_mean": boot_df["accuracy"].mean(),
        "accuracy_ci_low": percentile_ci(boot_df["accuracy"])[0],
        "accuracy_ci_high": percentile_ci(boot_df["accuracy"])[1],
    }

    return summary


if __name__ == "__main__":
    summaries = []

    for model_name, files in MODEL_FILES.items():
        print(f"\nRunning bootstrap for: {model_name}")
        summary = bootstrap_model(
            model_name=model_name,
            val_file=files["val"],
            test_file=files["test"],
            n_boot=1000,
            seed=42,
        )
        summaries.append(summary)

    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv(OUT_DIR / "bootstrap_q95_summary.csv", index=False)

    print("\nSaved:")
    print(OUT_DIR / "bootstrap_q95_summary.csv")
    print("\nBootstrap q95 summary:")
    print(summary_df)