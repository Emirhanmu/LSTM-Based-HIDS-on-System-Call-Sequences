from pathlib import Path
import pandas as pd


def main():
    out_dir = Path("results") / "final_tables"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Main q95 comparison
    q95_df = pd.DataFrame([
        {
            "model": "STIDE",
            "threshold_strategy": "validation-normal q95",
            "threshold": 0.145818,
            "precision": 0.9411764705882353,
            "recall": 0.8421052631578947,
            "f1": 0.8888888888888888,
            "fpr": 0.007874015748031496,
            "accuracy": 0.9726027397260274,
            "tp": 96,
            "tn": 756,
            "fp": 6,
            "fn": 18,
        },
        {
            "model": "LSTM Syscall",
            "threshold_strategy": "validation-normal q95",
            "threshold": 0.895200,
            "precision": 0.9469026548672567,
            "recall": 0.9385964912280702,
            "f1": 0.9427312775330398,
            "fpr": 0.007874015748031496,
            "accuracy": 0.9851598173515982,
            "tp": 107,
            "tn": 756,
            "fp": 6,
            "fn": 7,
        },
        {
            "model": "LSTM Context",
            "threshold_strategy": "validation-normal q95",
            "threshold": 1.067240,
            "precision": 0.8976377952755905,
            "recall": 1.0,
            "f1": 0.946058091286307,
            "fpr": 0.01706036745406824,
            "accuracy": 0.9851598173515982,
            "tp": 114,
            "tn": 749,
            "fp": 13,
            "fn": 0,
        },
    ])

    # Sensitivity q90 comparison
    q90_df = pd.DataFrame([
        {
            "model": "STIDE",
            "threshold_strategy": "validation-normal q90",
            "threshold": 0.054867,
            "precision": 0.9193548387096774,
            "recall": 1.0,
            "f1": 0.9579831932773109,
            "fpr": 0.013123359580052493,
            "accuracy": 0.9885844748858448,
            "tp": 114,
            "tn": 752,
            "fp": 10,
            "fn": 0,
        },
        {
            "model": "LSTM Syscall",
            "threshold_strategy": "validation-normal q90",
            "threshold": 0.575347,
            "precision": 0.9421487603305785,
            "recall": 1.0,
            "f1": 0.9702127659574468,
            "fpr": 0.009186351706036745,
            "accuracy": 0.9920091324200914,
            "tp": 114,
            "tn": 755,
            "fp": 7,
            "fn": 0,
        },
        {
            "model": "LSTM Context",
            "threshold_strategy": "validation-normal q90",
            "threshold": 0.915675,
            "precision": 0.8769230769230769,
            "recall": 1.0,
            "f1": 0.9344262295081968,
            "fpr": 0.02099737532808399,
            "accuracy": 0.9817351598173516,
            "tp": 114,
            "tn": 746,
            "fp": 16,
            "fn": 0,
        },
    ])

    # Original thresholding comparison
    original_df = pd.DataFrame([
        {
            "model": "STIDE",
            "threshold_strategy": "original validation-based",
            "precision": 0.9454545454545454,
            "recall": 0.9122807017543859,
            "f1": 0.9285714285714285,
            "fpr": 0.007874015748031496,
            "accuracy": 0.9817351598173516,
            "tp": 104,
            "tn": 756,
            "fp": 6,
            "fn": 10,
        },
        {
            "model": "LSTM Syscall",
            "threshold_strategy": "original validation-based",
            "precision": 0.9375,
            "recall": 0.2631578947368421,
            "f1": 0.4109589041095891,
            "fpr": 0.0026246719160104987,
            "accuracy": 0.9018264840182648,
            "tp": 30,
            "tn": 760,
            "fp": 2,
            "fn": 84,
        },
        {
            "model": "LSTM Context",
            "threshold_strategy": "original validation-based",
            "precision": 0.9174311926605505,
            "recall": 0.8771929824561403,
            "f1": 0.8968609865470852,
            "fpr": 0.011811023622047244,
            "accuracy": 0.973744292237443,
            "tp": 100,
            "tn": 753,
            "fp": 9,
            "fn": 14,
        },
    ])

    q95_df.to_csv(out_dir / "main_q95_comparison.csv", index=False)
    q90_df.to_csv(out_dir / "sensitivity_q90_comparison.csv", index=False)
    original_df.to_csv(out_dir / "original_thresholding_comparison.csv", index=False)

    print("\nSaved:")
    print(out_dir / "main_q95_comparison.csv")
    print(out_dir / "sensitivity_q90_comparison.csv")
    print(out_dir / "original_thresholding_comparison.csv")

    print("\nMain q95 comparison:")
    print(q95_df)


if __name__ == "__main__":
    main()