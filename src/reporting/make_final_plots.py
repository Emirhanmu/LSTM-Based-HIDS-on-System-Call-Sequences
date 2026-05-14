from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from sklearn.metrics import roc_auc_score, roc_curve, confusion_matrix, ConfusionMatrixDisplay


BASE_DIR = Path("data") / "processed" / "php_cwe_434"
OUT_DIR = Path("results") / "final_figures"
OUT_DIR.mkdir(parents=True, exist_ok=True)


MODEL_FILES = {
    "STIDE": BASE_DIR / "trace_level_stide_test_scores.csv",
    "LSTM Syscall": BASE_DIR / "trace_level_syscall_test_scores.csv",
    "LSTM Context": BASE_DIR / "trace_level_context_test_scores.csv",
}

Q95_THRESHOLDS = {
    "STIDE": 0.145818,
    "LSTM Syscall": 0.895200,
    "LSTM Context": 1.067240,
}


def load_scores(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=";")


def save_histogram(model_name: str, df: pd.DataFrame):
    normal = df[df["trace_label"] == 0]["trace_score"]
    anomaly = df[df["trace_label"] == 1]["trace_score"]

    plt.figure(figsize=(7, 4.5))
    plt.hist(normal, bins=30, alpha=0.7, label="Normal")
    plt.hist(anomaly, bins=30, alpha=0.7, label="Anomaly")
    plt.xlabel("Trace score")
    plt.ylabel("Count")
    plt.title(f"{model_name} - Trace Score Distribution")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_DIR / f"{model_name.lower().replace(' ', '_')}_score_hist.png", dpi=200)
    plt.close()


def save_confusion_matrix(model_name: str, df: pd.DataFrame, threshold: float):
    y_true = df["trace_label"]
    y_pred = (df["trace_score"] >= threshold).astype(int)

    cm = confusion_matrix(y_true, y_pred)
    disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=["Normal", "Anomaly"])

    fig, ax = plt.subplots(figsize=(5, 5))
    disp.plot(ax=ax, colorbar=False)
    ax.set_title(f"{model_name} - Confusion Matrix (q95)")
    plt.tight_layout()
    plt.savefig(OUT_DIR / f"{model_name.lower().replace(' ', '_')}_confusion_q95.png", dpi=200)
    plt.close()


def save_combined_roc():
    plt.figure(figsize=(7, 5))

    auc_rows = []

    for model_name, file_path in MODEL_FILES.items():
        df = load_scores(file_path)
        y_true = df["trace_label"]
        y_score = df["trace_score"]

        auc = roc_auc_score(y_true, y_score)
        fpr, tpr, _ = roc_curve(y_true, y_score)

        plt.plot(fpr, tpr, label=f"{model_name} (AUC={auc:.3f})")
        auc_rows.append({"model": model_name, "auc_roc": auc})

    plt.plot([0, 1], [0, 1], linestyle="--")
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("ROC Curves on Test Set")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_DIR / "combined_roc_curves.png", dpi=200)
    plt.close()

    pd.DataFrame(auc_rows).to_csv(OUT_DIR / "auc_roc_summary.csv", index=False)


def main():
    for model_name, file_path in MODEL_FILES.items():
        df = load_scores(file_path)
        save_histogram(model_name, df)
        save_confusion_matrix(model_name, df, Q95_THRESHOLDS[model_name])

    save_combined_roc()

    print("\nSaved figures to:")
    print(OUT_DIR)


if __name__ == "__main__":
    main()