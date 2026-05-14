from pathlib import Path
import pandas as pd


BASE_DIR = Path("data") / "processed" / "php_cwe_434"
OUT_DIR = Path("results") / "error_analysis"
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


def analyze_model(model_name: str, csv_path: Path, threshold: float):
    df = pd.read_csv(csv_path, sep=";").copy()

    df["prediction"] = (df["trace_score"] >= threshold).astype(int)

    fp_df = df[(df["trace_label"] == 0) & (df["prediction"] == 1)].copy()
    fn_df = df[(df["trace_label"] == 1) & (df["prediction"] == 0)].copy()
    tp_df = df[(df["trace_label"] == 1) & (df["prediction"] == 1)].copy()
    tn_df = df[(df["trace_label"] == 0) & (df["prediction"] == 0)].copy()

    fp_df = fp_df.sort_values("trace_score", ascending=False)
    fn_df = fn_df.sort_values("trace_score", ascending=True)
    tp_df = tp_df.sort_values("trace_score", ascending=False)
    tn_df = tn_df.sort_values("trace_score", ascending=False)

    fp_df.to_csv(OUT_DIR / f"{model_name.lower().replace(' ', '_')}_fp_q95.csv", index=False)
    fn_df.to_csv(OUT_DIR / f"{model_name.lower().replace(' ', '_')}_fn_q95.csv", index=False)
    tp_df.to_csv(OUT_DIR / f"{model_name.lower().replace(' ', '_')}_tp_q95.csv", index=False)
    tn_df.to_csv(OUT_DIR / f"{model_name.lower().replace(' ', '_')}_tn_q95.csv", index=False)

    summary = {
        "model": model_name,
        "threshold": threshold,
        "fp_count": len(fp_df),
        "fn_count": len(fn_df),
        "tp_count": len(tp_df),
        "tn_count": len(tn_df),
        "top_fp_examples": fp_df["trace_name"].head(5).tolist(),
        "top_fn_examples": fn_df["trace_name"].head(5).tolist(),
    }

    return summary


if __name__ == "__main__":
    summaries = []

    for model_name, csv_path in MODEL_FILES.items():
        summary = analyze_model(
            model_name=model_name,
            csv_path=csv_path,
            threshold=Q95_THRESHOLDS[model_name],
        )
        summaries.append(summary)

    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv(OUT_DIR / "error_analysis_q95_summary.csv", index=False)

    print(summary_df)