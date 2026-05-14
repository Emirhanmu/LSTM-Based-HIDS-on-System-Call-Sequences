from pathlib import Path
import json
import math
import time
import heapq

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset, DataLoader

from src.lstm.sequence_prediction_model import LSTMSequencePredictor


PAD_ID = 0


def get_device():
    if torch.cuda.is_available():
        return "cuda"
    elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def load_vocab_size(vocab_file: str) -> int:
    with open(vocab_file, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if "vocab_size" in payload:
        return int(payload["vocab_size"])
    if "token_to_id" in payload:
        return len(payload["token_to_id"])
    if "stoi" in payload:
        return len(payload["stoi"])

    raise ValueError(f"Could not infer vocab size from: {vocab_file}")


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


def select_threshold(validation_trace_df: pd.DataFrame, max_fpr_constraint: float = 0.20, num_thresholds: int = 300):
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


def parse_encoded_sequence(seq_text: str):
    if pd.isna(seq_text):
        return []
    return [int(x) for x in str(seq_text).strip().split()]


def detect_encoded_column(df_or_columns):
    if isinstance(df_or_columns, pd.DataFrame):
        cols = df_or_columns.columns.tolist()
    else:
        cols = list(df_or_columns)

    candidates = [
        "encoded_context",
        "encoded_contexts",
        "encoded_tokens",
        "encoded_sequence",
        "encoded_sequences",
    ]
    for c in candidates:
        if c in cols:
            return c

    raise ValueError(f"Could not find encoded context column. Available columns: {cols}")


class EncodedContextDataset(Dataset):
    def __init__(self, df: pd.DataFrame, encoded_col: str):
        self.trace_names = df["trace_name"].tolist()
        self.labels = df["label"].astype(int).tolist()
        self.sequences = [parse_encoded_sequence(x) for x in df[encoded_col].tolist()]

        self.inputs = []
        self.targets = []
        self.kept_trace_names = []
        self.kept_labels = []

        for trace_name, label, seq in zip(self.trace_names, self.labels, self.sequences):
            if len(seq) < 3:
                continue
            self.inputs.append(seq[:-1])
            self.targets.append(seq[1:])
            self.kept_trace_names.append(trace_name)
            self.kept_labels.append(label)

    def __len__(self):
        return len(self.inputs)

    def __getitem__(self, idx):
        return {
            "input_ids": torch.tensor(self.inputs[idx], dtype=torch.long),
            "target_ids": torch.tensor(self.targets[idx], dtype=torch.long),
            "trace_name": self.kept_trace_names[idx],
            "label": self.kept_labels[idx],
        }


def collate_fn(batch):
    input_ids = [item["input_ids"] for item in batch]
    target_ids = [item["target_ids"] for item in batch]
    trace_names = [item["trace_name"] for item in batch]
    labels = torch.tensor([item["label"] for item in batch], dtype=torch.long)

    padded_inputs = torch.nn.utils.rnn.pad_sequence(
        input_ids, batch_first=True, padding_value=PAD_ID
    )
    padded_targets = torch.nn.utils.rnn.pad_sequence(
        target_ids, batch_first=True, padding_value=PAD_ID
    )

    return {
        "input_ids": padded_inputs,
        "target_ids": padded_targets,
        "trace_names": trace_names,
        "labels": labels,
    }


def score_batch(model, batch, device="cpu", pad_id: int = 0):
    import torch.nn.functional as F

    input_ids = batch["input_ids"].to(device)
    target_ids = batch["target_ids"].to(device)

    with torch.no_grad():
        logits = model(input_ids)
        log_probs = F.log_softmax(logits, dim=-1)

        true_token_log_probs = log_probs.gather(
            dim=2,
            index=target_ids.unsqueeze(-1)
        ).squeeze(-1)

        nll = -true_token_log_probs
        mask = (target_ids != pad_id).float()

        seq_nll_sum = (nll * mask).sum(dim=1)
        seq_token_count = mask.sum(dim=1).clamp(min=1)
        seq_scores = seq_nll_sum / seq_token_count

    return seq_scores.cpu().tolist()


def aggregate_trace_scores_full(window_df: pd.DataFrame, top_fraction: float = 0.10) -> pd.DataFrame:
    rows = []

    for trace_name, group in window_df.groupby("trace_name"):
        scores = group["window_score"].sort_values(ascending=False).to_numpy()
        k = max(1, math.ceil(len(scores) * top_fraction))
        trace_score = float(scores[:k].mean())
        trace_label = int(group["label"].max())

        rows.append({
            "trace_name": trace_name,
            "trace_label": trace_label,
            "num_windows": len(group),
            "top_k_used": k,
            "trace_score": trace_score,
        })

    return pd.DataFrame(rows).sort_values("trace_name").reset_index(drop=True)


def build_validation_trace_df(model, validation_csv, device, batch_size=512):
    print("\nScoring validation set...")
    df = pd.read_csv(validation_csv, sep=";")
    encoded_col = detect_encoded_column(df)

    ds = EncodedContextDataset(df, encoded_col=encoded_col)

    dl = DataLoader(
        ds,
        batch_size=batch_size,
        shuffle=False,
        collate_fn=collate_fn,
        num_workers=0,
        pin_memory=(device == "cuda"),
    )

    rows = []
    seen = 0
    start = time.time()

    for batch in dl:
        scores = score_batch(model, batch, device=device, pad_id=PAD_ID)

        for trace_name, label, score in zip(batch["trace_names"], batch["labels"].tolist(), scores):
            rows.append({
                "trace_name": trace_name,
                "label": label,
                "window_score": score,
            })

        seen += len(scores)
        if seen % 100_000 == 0:
            elapsed = time.time() - start
            print(f"Validation processed windows: {seen:,} | elapsed: {elapsed/60:.1f} min")

    window_df = pd.DataFrame(rows)
    return aggregate_trace_scores_full(window_df, top_fraction=0.10)


def count_test_windows_per_trace(test_csv, chunksize=200_000):
    print("\nCounting test windows per trace...")
    counts = {}
    labels = {}
    total_rows = 0
    start = time.time()

    for chunk_idx, chunk in enumerate(
        pd.read_csv(test_csv, sep=";", usecols=["trace_name", "label"], chunksize=chunksize),
        start=1
    ):
        vc = chunk["trace_name"].value_counts()
        for trace_name, cnt in vc.items():
            counts[trace_name] = counts.get(trace_name, 0) + int(cnt)

        label_map = chunk.groupby("trace_name")["label"].max().to_dict()
        labels.update(label_map)

        total_rows += len(chunk)

        if chunk_idx % 20 == 0:
            elapsed = time.time() - start
            print(f"Count pass rows: {total_rows:,} | traces: {len(counts):,} | elapsed: {elapsed/60:.1f} min")

    print(f"Finished count pass. Total rows: {total_rows:,}, total traces: {len(counts):,}")
    return counts, labels


def streaming_test_trace_df(model, test_csv, device, top_fraction=0.10, chunksize=50_000, batch_size=512):
    counts, labels = count_test_windows_per_trace(test_csv, chunksize=200_000)

    sample_df = pd.read_csv(test_csv, sep=";", nrows=1)
    encoded_col = detect_encoded_column(sample_df)

    k_map = {
        trace_name: max(1, math.ceil(cnt * top_fraction))
        for trace_name, cnt in counts.items()
    }

    heaps = {trace_name: [] for trace_name in counts.keys()}

    total_rows = 0
    start = time.time()
    total_expected = sum(counts.values())

    print("\nScoring test set with streaming top-k aggregation...")

    for chunk_idx, chunk in enumerate(
        pd.read_csv(test_csv, sep=";", chunksize=chunksize),
        start=1
    ):
        ds = EncodedContextDataset(chunk, encoded_col=encoded_col)

        dl = DataLoader(
            ds,
            batch_size=batch_size,
            shuffle=False,
            collate_fn=collate_fn,
            num_workers=0,
            pin_memory=(device == "cuda"),
        )

        chunk_rows = 0

        for batch in dl:
            scores = score_batch(model, batch, device=device, pad_id=PAD_ID)

            for trace_name, score in zip(batch["trace_names"], scores):
                heap = heaps[trace_name]
                k = k_map[trace_name]

                if len(heap) < k:
                    heapq.heappush(heap, score)
                else:
                    if score > heap[0]:
                        heapq.heapreplace(heap, score)

            chunk_rows += len(scores)

        total_rows += chunk_rows

        if chunk_idx % 20 == 0:
            elapsed = time.time() - start
            rate = total_rows / max(elapsed, 1e-9)
            remaining = total_expected - total_rows
            eta_min = (remaining / rate) / 60 if rate > 0 else -1

            print(
                f"Test processed rows: {total_rows:,} / {total_expected:,} "
                f"| traces: {len(heaps):,} | elapsed: {elapsed/60:.1f} min | ETA: {eta_min:.1f} min"
            )

    rows = []
    for trace_name, heap in heaps.items():
        if not heap:
            continue
        trace_score = float(np.mean(heap))
        rows.append({
            "trace_name": trace_name,
            "trace_label": int(labels[trace_name]),
            "num_windows": counts[trace_name],
            "top_k_used": k_map[trace_name],
            "trace_score": trace_score,
        })

    trace_df = pd.DataFrame(rows).sort_values("trace_name").reset_index(drop=True)
    return trace_df


if __name__ == "__main__":
    device = get_device()
    print(f"Using device: {device}")

    base_dir = Path("data") / "processed" / "php_cwe_434"

    vocab_file = base_dir / "context_vocab.json"
    model_file = base_dir / "lstm_context_sequence_predictor.pt"
    validation_csv = base_dir / "validation_encoded_context.csv"
    test_csv = base_dir / "test_encoded_context.csv"

    output_val = base_dir / "trace_level_context_validation_scores.csv"
    output_test = base_dir / "trace_level_context_test_scores.csv"
    output_summary = base_dir / "trace_level_context_eval_summary.csv"

    vocab_size = load_vocab_size(str(vocab_file))

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

    validation_trace_df = build_validation_trace_df(
        model=model,
        validation_csv=str(validation_csv),
        device=device,
        batch_size=512,
    )

    test_trace_df = streaming_test_trace_df(
        model=model,
        test_csv=str(test_csv),
        device=device,
        top_fraction=0.10,
        chunksize=50_000,
        batch_size=512,
    )

    validation_trace_df.to_csv(output_val, index=False, sep=";")
    test_trace_df.to_csv(output_test, index=False, sep=";")

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
    print("TRACE-LEVEL CONTEXT LSTM RESULTS")
    print("====================")
    print(f"Best threshold: {best_threshold:.6f}")

    print("\nValidation metrics:")
    for k, v in val_metrics.items():
        print(f"{k}: {v}")

    print("\nTest metrics:")
    for k, v in test_metrics.items():
        print(f"{k}: {v}")

    summary_df = pd.DataFrame([{
        "method": "LSTM_CONTEXT_TRACE_LEVEL",
        "top_fraction": 0.10,
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

    print(f"\nSaved validation trace scores to: {output_val}")
    print(f"Saved test trace scores to: {output_test}")
    print(f"Saved summary to: {output_summary}")