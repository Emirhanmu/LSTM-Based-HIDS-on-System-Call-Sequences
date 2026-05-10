import json
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader

from src.lstm.sequence_prediction_dataset import (
    SequencePredictionDataset,
    collate_sequence_prediction_fn,
)
from src.lstm.sequence_prediction_model import LSTMSequencePredictor


PAD_ID = 0


def load_vocab_size(vocab_file: str) -> int:
    with open(vocab_file, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return int(payload["vocab_size"])


def score_dataset(model, dataloader, device="cpu", pad_id: int = 0) -> pd.DataFrame:
    model.eval()

    all_scores = []
    all_labels = []

    with torch.no_grad():
        for batch in dataloader:
            input_ids = batch["input_ids"].to(device)      # [B, L]
            target_ids = batch["target_ids"].to(device)    # [B, L]
            labels = batch["labels"].to(device)            # [B]

            logits = model(input_ids)                      # [B, L, V]
            log_probs = F.log_softmax(logits, dim=-1)      # [B, L, V]

            true_token_log_probs = log_probs.gather(
                dim=2,
                index=target_ids.unsqueeze(-1)
            ).squeeze(-1)                                  # [B, L]

            nll = -true_token_log_probs                    # [B, L]

            mask = (target_ids != pad_id).float()          # [B, L]
            seq_nll_sum = (nll * mask).sum(dim=1)          # [B]
            seq_token_count = mask.sum(dim=1).clamp(min=1) # [B]

            # window anomaly score = ortalama NLL
            seq_scores = seq_nll_sum / seq_token_count     # [B]

            all_scores.extend(seq_scores.cpu().tolist())
            all_labels.extend(labels.cpu().tolist())

    return pd.DataFrame({
        "label": all_labels,
        "score": all_scores,
    })


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


def select_threshold(
    validation_df: pd.DataFrame,
    max_fpr_constraint: float = 0.20,
    num_thresholds: int = 200,
) -> tuple[float, dict]:
    print("\n====================")
    print("VALIDATION LABEL COUNTS")
    print("====================")
    print(validation_df["label"].value_counts(dropna=False))

    min_score = float(validation_df["score"].min())
    max_score = float(validation_df["score"].max())

    thresholds = np.linspace(min_score, max_score, num_thresholds)

    best_threshold = None
    best_metrics = None

    # Önce FPR constraint'i sağlayanlar arasında en iyi F1'i ara
    for threshold in thresholds:
        y_pred = (validation_df["score"] >= threshold).astype(int)
        metrics = compute_metrics(validation_df["label"], y_pred)

        if metrics["fpr"] <= max_fpr_constraint:
            if best_metrics is None:
                best_threshold = float(threshold)
                best_metrics = metrics
            else:
                current_key = (metrics["f1"], metrics["recall"], metrics["precision"])
                best_key = (best_metrics["f1"], best_metrics["recall"], best_metrics["precision"])
                if current_key > best_key:
                    best_threshold = float(threshold)
                    best_metrics = metrics

    # Eğer hiçbiri constraint'i sağlamazsa, tüm threshold'lar arasında en iyi F1
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

    print("\n====================")
    print("THRESHOLD SELECTION")
    print("====================")
    print(f"Score range: min={min_score:.6f}, max={max_score:.6f}")
    print(f"Threshold candidates: {num_thresholds}")
    print(f"Max FPR constraint: {max_fpr_constraint}")
    print(f"Best threshold: {best_threshold:.6f}")
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
    print(f"Threshold: {threshold:.6f}")
    for k, v in metrics.items():
        print(f"{k}: {v}")

    return metrics


def save_summary(
    output_file: str,
    scenario: str,
    threshold: float,
    val_metrics: dict,
    test_metrics: dict,
    val_rows_used: int,
    test_rows_used: int,
):
    summary_df = pd.DataFrame([{
        "scenario": scenario,
        "method": "LSTM_SEQUENCE_PREDICTOR",
        "threshold": threshold,
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
    print(f"\nSaved sequence predictor evaluation summary to: {output_file}")


if __name__ == "__main__":
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")

    scenario = "php_cwe_434"

    vocab_file = r"data\processed\php_cwe_434\syscall_vocab.json"
    model_file = r"data\processed\php_cwe_434\lstm_sequence_predictor.pt"
    validation_csv = r"data\processed\php_cwe_434\validation_encoded_syscalls.csv"
    test_csv = r"data\processed\php_cwe_434\test_encoded_syscalls.csv"

    output_summary = r"data\processed\php_cwe_434\lstm_sequence_predictor_eval_summary.csv"

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

    validation_scores = score_dataset(model, validation_loader, device=device, pad_id=PAD_ID)
    test_scores = score_dataset(model, test_loader, device=device, pad_id=PAD_ID)

    best_threshold, val_metrics = select_threshold(
        validation_df=validation_scores,
        max_fpr_constraint=0.20,
        num_thresholds=200,
    )

    test_metrics = evaluate_with_threshold(test_scores, best_threshold, name="test")

    save_summary(
        output_file=output_summary,
        scenario=scenario,
        threshold=best_threshold,
        val_metrics=val_metrics,
        test_metrics=test_metrics,
        val_rows_used=len(validation_dataset),
        test_rows_used=len(test_dataset),
    )