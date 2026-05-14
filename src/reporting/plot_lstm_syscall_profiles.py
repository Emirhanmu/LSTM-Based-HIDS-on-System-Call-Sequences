from pathlib import Path
import math
import time

import numpy as np
import pandas as pd
import torch
import matplotlib.pyplot as plt
from torch.utils.data import Dataset, DataLoader

from src.lstm.sequence_prediction_model import LSTMSequencePredictor


BASE_DIR = Path("data") / "processed" / "php_cwe_434"
ERR_DIR = Path("results") / "error_analysis"
OUT_DIR = Path("results") / "profiles"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PAD_ID = 0
Q95_THRESHOLD = 0.895200


def get_device():
    if torch.cuda.is_available():
        return "cuda"
    elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def load_vocab_size(vocab_file: Path) -> int:
    import json
    with open(vocab_file, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if "vocab_size" in payload:
        return int(payload["vocab_size"])
    if "token_to_id" in payload:
        return len(payload["token_to_id"])
    if "stoi" in payload:
        return len(payload["stoi"])

    raise ValueError(f"Could not infer vocab size from {vocab_file}")


def parse_encoded_sequence(seq_text: str):
    if pd.isna(seq_text):
        return []
    return [int(x) for x in str(seq_text).strip().split()]


def score_sequences(model, sequences, device="cpu", batch_size=512):
    class SeqDataset(Dataset):
        def __init__(self, seqs):
            self.inputs = []
            self.targets = []
            for seq in seqs:
                if len(seq) < 3:
                    self.inputs.append([])
                    self.targets.append([])
                else:
                    self.inputs.append(seq[:-1])
                    self.targets.append(seq[1:])

        def __len__(self):
            return len(self.inputs)

        def __getitem__(self, idx):
            return {
                "input_ids": torch.tensor(self.inputs[idx], dtype=torch.long),
                "target_ids": torch.tensor(self.targets[idx], dtype=torch.long),
            }

    def collate_fn(batch):
        input_ids = [b["input_ids"] for b in batch]
        target_ids = [b["target_ids"] for b in batch]

        padded_inputs = torch.nn.utils.rnn.pad_sequence(
            input_ids, batch_first=True, padding_value=PAD_ID
        )
        padded_targets = torch.nn.utils.rnn.pad_sequence(
            target_ids, batch_first=True, padding_value=PAD_ID
        )
        return {"input_ids": padded_inputs, "target_ids": padded_targets}

    ds = SeqDataset(sequences)
    dl = DataLoader(
        ds,
        batch_size=batch_size,
        shuffle=False,
        collate_fn=collate_fn,
        num_workers=0,
        pin_memory=(device == "cuda"),
    )

    import torch.nn.functional as F

    all_scores = []
    model.eval()

    for batch in dl:
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
            mask = (target_ids != PAD_ID).float()

            seq_nll_sum = (nll * mask).sum(dim=1)
            seq_token_count = mask.sum(dim=1).clamp(min=1)
            seq_scores = seq_nll_sum / seq_token_count

        all_scores.extend(seq_scores.cpu().tolist())

    return all_scores


def pick_example_traces():
    fp_file = ERR_DIR / "lstm_syscall_fp_q95.csv"
    fn_file = ERR_DIR / "lstm_syscall_fn_q95.csv"
    tp_file = ERR_DIR / "lstm_syscall_tp_q95.csv"
    tn_file = ERR_DIR / "lstm_syscall_tn_q95.csv"

    fp_df = pd.read_csv(fp_file)
    fn_df = pd.read_csv(fn_file)
    tp_df = pd.read_csv(tp_file)
    tn_df = pd.read_csv(tn_file)

    selected = {
        "FP": fp_df.iloc[0]["trace_name"] if len(fp_df) > 0 else None,
        "FN": fn_df.iloc[0]["trace_name"] if len(fn_df) > 0 else None,
        "TP": tp_df.iloc[0]["trace_name"] if len(tp_df) > 0 else None,
        "TN": tn_df.iloc[0]["trace_name"] if len(tn_df) > 0 else None,
    }
    return selected


def load_trace_windows(trace_names):
    trace_names = set(trace_names)
    usecols = ["trace_name", "label", "window_id", "window_start_idx", "window_syscalls"]

    parts = []
    for chunk in pd.read_csv(BASE_DIR / "test_windows.csv", sep=";", usecols=usecols, chunksize=200_000):
        sub = chunk[chunk["trace_name"].isin(trace_names)]
        if not sub.empty:
            parts.append(sub)

    if not parts:
        raise ValueError("No matching traces found in test_windows.csv")

    df = pd.concat(parts, ignore_index=True)
    return df


def load_trace_encoded(trace_names):
    trace_names = set(trace_names)
    parts = []
    cols = None

    for i, chunk in enumerate(pd.read_csv(BASE_DIR / "test_encoded_syscalls.csv", sep=";", chunksize=200_000)):
        if cols is None:
            cols = chunk.columns.tolist()

        sub = chunk[chunk["trace_name"].isin(trace_names)]
        if not sub.empty:
            parts.append(sub)

    if not parts:
        raise ValueError("No matching traces found in test_encoded_syscalls.csv")

    df = pd.concat(parts, ignore_index=True)
    return df


def merge_trace_data(windows_df, encoded_df):
    # Prefer explicit key if present
    candidate_keys = ["global_window_id", "window_id", "window_start_idx"]

    for key in candidate_keys:
        if key in windows_df.columns and key in encoded_df.columns:
            merged = windows_df.merge(
                encoded_df[["trace_name", key, "encoded_syscalls"]],
                on=["trace_name", key],
                how="inner",
            )
            if not merged.empty:
                return merged, f"merged_on_{key}"

    # fallback: positional match inside each trace
    rows = []
    for trace_name, w_group in windows_df.groupby("trace_name"):
        e_group = encoded_df[encoded_df["trace_name"] == trace_name].copy()

        w_group = w_group.sort_values(["window_start_idx", "window_id"]).reset_index(drop=True)
        e_group = e_group.reset_index(drop=True)

        n = min(len(w_group), len(e_group))
        tmp = w_group.iloc[:n].copy()
        tmp["encoded_syscalls"] = e_group.iloc[:n]["encoded_syscalls"].values
        rows.append(tmp)

    merged = pd.concat(rows, ignore_index=True)
    return merged, "positional_fallback"


def compute_trace_score(window_scores):
    scores = np.array(window_scores, dtype=float)
    k = max(1, math.ceil(len(scores) * 0.10))
    top_scores = np.sort(scores)[::-1][:k]
    return float(top_scores.mean()), k


def plot_trace_profile(df_trace, trace_name, trace_type):
    df_trace = df_trace.sort_values(["window_start_idx", "window_id"]).reset_index(drop=True)

    trace_score, top_k = compute_trace_score(df_trace["window_score"].tolist())

    true_trace_label = 1 if trace_type in ["TP", "FN"] else 0
    prediction = int(trace_score >= Q95_THRESHOLD)

    x = np.arange(len(df_trace))

    plt.figure(figsize=(9, 4.5))
    plt.plot(x, df_trace["window_score"].to_numpy(), linewidth=1.5, label="Window score")
    plt.axhline(Q95_THRESHOLD, linestyle="--", linewidth=1.5, label="q95 threshold")
    plt.axhline(trace_score, linestyle=":", linewidth=1.5, label="Trace score (top-10% mean)")

    title = (
        f"LSTM Syscall Profile - {trace_type} - {trace_name}\n"
        f"trace_label={true_trace_label}, "
        f"trace_score={trace_score:.4f}, threshold={Q95_THRESHOLD:.4f}, top_k={top_k}"
    )
    plt.title(title)
    plt.xlabel("Window index within trace")
    plt.ylabel("Window score")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_DIR / f"{trace_type.lower()}_{trace_name}_profile.png", dpi=200)
    plt.close()

    return {
        "trace_type": trace_type,
        "trace_name": trace_name,
        "label": true_trace_label,
        "num_windows": len(df_trace),
        "trace_score": trace_score,
        "threshold": Q95_THRESHOLD,
        "prediction": prediction,
        "top_k_used": top_k,
    }


if __name__ == "__main__":
    selected = pick_example_traces()
    print("Selected traces:")
    print(selected)

    trace_names = [t for t in selected.values() if t is not None]

    windows_df = load_trace_windows(trace_names)
    encoded_df = load_trace_encoded(trace_names)
    merged_df, merge_mode = merge_trace_data(windows_df, encoded_df)

    print(f"Merge mode: {merge_mode}")
    print(f"Merged rows: {len(merged_df)}")

    merged_df["encoded_syscalls_list"] = merged_df["encoded_syscalls"].apply(parse_encoded_sequence)

    device = get_device()
    print(f"Using device: {device}")

    vocab_size = load_vocab_size(BASE_DIR / "syscall_vocab.json")

    model = LSTMSequencePredictor(
        vocab_size=vocab_size,
        embedding_dim=64,
        hidden_dim=128,
        num_layers=1,
        dropout=0.2,
        pad_idx=PAD_ID,
    ).to(device)

    model.load_state_dict(torch.load(BASE_DIR / "lstm_sequence_predictor.pt", map_location=device))
    model.eval()
    print("Model loaded.")

    summaries = []

    for trace_type, trace_name in selected.items():
        if trace_name is None:
            continue

        sub = merged_df[merged_df["trace_name"] == trace_name].copy()
        sub = sub.sort_values(["window_start_idx", "window_id"]).reset_index(drop=True)

        scores = score_sequences(
            model=model,
            sequences=sub["encoded_syscalls_list"].tolist(),
            device=device,
            batch_size=512,
        )
        sub["window_score"] = scores

        summary = plot_trace_profile(sub, trace_name, trace_type)
        summaries.append(summary)

        # suspicious windows
        suspicious = sub.sort_values("window_score", ascending=False).head(10)[
            ["trace_name", "label", "window_id", "window_start_idx", "window_score", "window_syscalls"]
        ]
        suspicious.to_csv(OUT_DIR / f"{trace_type.lower()}_{trace_name}_top_windows.csv", index=False)

    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv(OUT_DIR / "lstm_syscall_profile_summary.csv", index=False)

    print("\nSaved profile plots and summaries to:")
    print(OUT_DIR)
    print("\nProfile summary:")
    print(summary_df)