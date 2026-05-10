import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from torch.utils.data import DataLoader

from src.lstm.context_sequence_prediction_dataset import (
    ContextSequencePredictionDataset,
    collate_context_sequence_prediction_fn,
)
from src.lstm.sequence_prediction_model import LSTMSequencePredictor


PAD_ID = 0


def load_vocab_size(vocab_file: str) -> int:
    with open(vocab_file, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return int(payload["vocab_size"])


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


def score_windows(model, dataloader, device="cpu", pad_id: int = 0) -> list[float]:
    import torch.nn.functional as F

    model.eval()
    all_scores = []

    with torch.no_grad():
        for batch in dataloader:
            input_ids = batch["input_ids"].to(device)
            target_ids = batch["target_ids"].to(device)

            logits = model(input_ids)                  # [B, L, V]
            log_probs = F.log_softmax(logits, dim=-1)  # [B, L, V]

            true_token_log_probs = log_probs.gather(
                dim=2,
                index=target_ids.unsqueeze(-1)
            ).squeeze(-1)                              # [B, L]

            nll = -true_token_log_probs

            mask = (target_ids != pad_id).float()
            seq_nll_sum = (nll * mask).sum(dim=1)
            seq_token_count = mask.sum(dim=1).clamp(min=1)

            # window score = mean NLL over the sequence
            seq_scores = seq_nll_sum / seq_token_count

            all_scores.extend(seq_scores.cpu().tolist())

    return all_scores


def build_window_score_df(csv_path: str, window_scores: list[float]) -> pd.DataFrame:
    meta_df = pd.read_csv(csv_path, sep=";", usecols=["trace_name", "label"])
    if len(meta_df) != len(window_scores):
        raise ValueError(f"Metadata rows ({len(meta_df)}) and score rows ({len(window_scores)}) do not match.")

    meta_df = meta_df.copy()
    meta_df["window_score"] = window_scores
    return meta_df


def aggregate_trace_scores(window_df: pd.DataFrame, top_fraction: float = 0.10) -> pd.DataFrame:
    rows = []

    for trace_name, group in window_df.groupby("trace_name"):
        scores = group["window_score"].sort_values(ascending=False).to_numpy()
        k = max(1, math.ceil(len(scores) * top_fraction))
        trace_score = float(scores[:k].mean())

        # trace label = trace içinde en az bir anomalik pencere varsa 1
        trace_label = int(group["label"].max())

        rows.append({
            "trace_name": trace_name,
            "trace_label": trace_label,
            "num_windows": len(group),
            "top_k_used": k,
            "trace_score": trace_score,
        })

    trace_df = pd.DataFrame(rows).sort_values("trace_name").reset_index(drop=True)
    return trace_df


def select_threshold(
    validation_trace_df: pd.DataFrame,
    max_fpr_constraint: float = 0.20,
    num_thresholds: int = 300,
):
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

    # constraint altında hiç threshold kalmazsa fallback: en iyi F1
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


if __name__ == "__main__":
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")

    vocab_file = r"data\processed\php_cwe_434\context_vocab.json"
    model_file = r"data\processed\php_cwe_434\lstm_context_sequence_predictor.pt"
    validation_csv = r"data\processed\php_cwe_434\validation_encoded_context.csv"
    test_csv = r"data\processed\php_cwe_434\test_encoded_context.csv"

    output_trace_val = r"data\processed\php_cwe_434\trace_level_context_validation_scores.csv"
    output_trace_test = r"data\processed\php_cwe_434\trace_level_context_test_scores.csv"
    output_summary = r"data\processed\php_cwe_434\trace_level_context_eval_summary.csv"

    top_fraction = 0.10

    vocab_size = load_vocab_size(vocab_file)

    validation_dataset = ContextSequencePredictionDataset(
        validation_csv,
        max_rows=None,
        only_normal=False,
    )

    test_dataset = ContextSequencePredictionDataset(
        test_csv,
        max_rows=None,
        only_normal=False,
    )

    validation_loader = DataLoader(
        validation_dataset,
        batch_size=256,
        shuffle=False,
        collate_fn=collate_context_sequence_prediction_fn,
    )

    test_loader = DataLoader(
        test_dataset,
        batch_size=256,
        shuffle=False,
        collate_fn=collate_context_sequence_prediction_fn,
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
    print("Model loaded.")

    print("\nScoring validation windows...")
    validation_window_scores = score_windows(model, validation_loader, device=device, pad_id=PAD_ID)

    print("Scoring test windows...")
    test_window_scores = score_windows(model, test_loader, device=device, pad_id=PAD_ID)

    validation_window_df = build_window_score_df(validation_csv, validation_window_scores)
    test_window_df = build_window_score_df(test_csv, test_window_scores)

    validation_trace_df = aggregate_trace_scores(validation_window_df, top_fraction=top_fraction)
    test_trace_df = aggregate_trace_scores(test_window_df, top_fraction=top_fraction)

    Path(output_trace_val).parent.mkdir(parents=True, exist_ok=True)
    validation_trace_df.to_csv(output_trace_val, index=False, sep=";")
    test_trace_df.to_csv(output_trace_test, index=False, sep=";")

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
    print("TRACE-LEVEL RESULTS")
    print("====================")
    print(f"Top fraction: {top_fraction}")
    print(f"Best threshold: {best_threshold:.6f}")

    print("\nValidation metrics:")
    for k, v in val_metrics.items():
        print(f"{k}: {v}")

    print("\nTest metrics:")
    for k, v in test_metrics.items():
        print(f"{k}: {v}")

    summary_df = pd.DataFrame([{
        "method": "LSTM_CONTEXT_TRACE_LEVEL",
        "top_fraction": top_fraction,
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
    print(f"\nSaved validation trace scores to: {output_trace_val}")
    print(f"Saved test trace scores to: {output_trace_test}")
    print(f"Saved summary to: {output_summary}")