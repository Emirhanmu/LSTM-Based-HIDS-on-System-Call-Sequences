import json
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from torch.utils.data import DataLoader

from src.lstm.sequence_prediction_dataset import (
    SequencePredictionDataset,
    collate_sequence_prediction_fn,
)
from src.lstm.sequence_prediction_model import LSTMSequencePredictor
from src.lstm.evaluate_sequence_predictor import score_dataset, compute_metrics


PAD_ID = 0


def load_vocab_size(vocab_file: str) -> int:
    with open(vocab_file, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return int(payload["vocab_size"])


def select_threshold_with_constraint(
    validation_df: pd.DataFrame,
    max_fpr_constraint: float | None,
    num_thresholds: int = 200,
):
    min_score = float(validation_df["score"].min())
    max_score = float(validation_df["score"].max())
    thresholds = np.linspace(min_score, max_score, num_thresholds)

    best_threshold = None
    best_metrics = None

    for threshold in thresholds:
        y_pred = (validation_df["score"] >= threshold).astype(int)
        metrics = compute_metrics(validation_df["label"], y_pred)

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

    # constraint yüzünden hiç threshold kalmadıysa, tüm threshold'larda en iyi F1
    if best_metrics is None:
        for threshold in thresholds:
            y_pred = (validation_df["score"] >= threshold).astype(int)
            metrics = compute_metrics(validation_df["label"], y_pred)

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
    y_pred = (df["score"] >= threshold).astype(int)
    return compute_metrics(df["label"], y_pred)


if __name__ == "__main__":
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")

    vocab_file = r"data\processed\php_cwe_434\syscall_vocab.json"
    model_file = r"data\processed\php_cwe_434\lstm_sequence_predictor.pt"
    validation_csv = r"data\processed\php_cwe_434\validation_encoded_syscalls.csv"
    test_csv = r"data\processed\php_cwe_434\test_encoded_syscalls.csv"

    output_file = r"data\processed\php_cwe_434\lstm_sequence_threshold_sweep.csv"

    vocab_size = load_vocab_size(vocab_file)

    validation_dataset = SequencePredictionDataset(
        validation_csv,
        max_rows=20_000,
        only_normal=False,
    )

    test_dataset = SequencePredictionDataset(
        test_csv,
        max_rows=20_000,
        only_normal=False,
    )

    validation_loader = DataLoader(
        validation_dataset,
        batch_size=128,
        shuffle=False,
        collate_fn=collate_sequence_prediction_fn,
    )

    test_loader = DataLoader(
        test_dataset,
        batch_size=128,
        shuffle=False,
        collate_fn=collate_sequence_prediction_fn,
    )

    model = LSTMSequencePredictor(
        vocab_size=vocab_size,
        embedding_dim=64,
        hidden_dim=128,
        num_layers=1,
        dropout=0.2,
        pad_idx=PAD_ID,
    ).to(device)

    model.load_state_dict(torch.load(model_file, map_location=device))
    model.eval()

    print("\nScoring validation set...")
    validation_scores = score_dataset(model, validation_loader, device=device, pad_id=PAD_ID)

    print("\nScoring test set...")
    test_scores = score_dataset(model, test_loader, device=device, pad_id=PAD_ID)

    print("\nValidation label counts:")
    print(validation_scores["label"].value_counts(dropna=False))

    constraints = [None, 0.50, 0.40, 0.30, 0.20, 0.10, 0.05]
    rows = []

    for constraint in constraints:
        threshold, val_metrics = select_threshold_with_constraint(
            validation_df=validation_scores,
            max_fpr_constraint=constraint,
            num_thresholds=300,
        )

        test_metrics = evaluate_with_threshold(test_scores, threshold)

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
        print(f"VAL -> F1: {val_metrics['f1']:.4f}, Recall: {val_metrics['recall']:.4f}, Precision: {val_metrics['precision']:.4f}, FPR: {val_metrics['fpr']:.4f}")
        print(f"TEST -> F1: {test_metrics['f1']:.4f}, Recall: {test_metrics['recall']:.4f}, Precision: {test_metrics['precision']:.4f}, FPR: {test_metrics['fpr']:.4f}, Acc: {test_metrics['accuracy']:.4f}")

    result_df = pd.DataFrame(rows)
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(output_file, index=False, sep=";")

    print(f"\nSaved threshold sweep results to: {output_file}")
    print("\nTop results by test_f1:")
    print(result_df.sort_values("test_f1", ascending=False).head(10))