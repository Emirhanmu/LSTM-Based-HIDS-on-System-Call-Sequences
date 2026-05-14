from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import io
import json
import math
import pickle
import tempfile
import zipfile


import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset, DataLoader
from src.pipeline.process_single_trace import process_single_trace
from src.lstm.sequence_prediction_model import LSTMSequencePredictor


# =========================
# Paths and constants
# =========================
BASE_DIR = Path("data") / "processed" / "php_cwe_434"
ARTIFACT_DIR = Path("data") / "artifacts"

SYSCALL_MODEL_FILE = BASE_DIR / "lstm_sequence_predictor.pt"
CONTEXT_MODEL_FILE = BASE_DIR / "lstm_context_sequence_predictor.pt"
SYSCALL_VOCAB_FILE = BASE_DIR / "syscall_vocab.json"
CONTEXT_VOCAB_FILE = BASE_DIR / "context_vocab.json"
STIDE_ARTIFACT_FILE = ARTIFACT_DIR / "stide_memory_6gram.pkl"

WINDOW_SIZE = 30
STRIDE = 1
TOP_FRACTION = 0.10
PAD_ID = 0

THRESHOLDS = {
    "q95": {
        "STIDE": 0.145818,
        "LSTM Syscall": 0.8952,
        "LSTM Context": 1.06724,
    },
    "q90": {
        "STIDE": 0.054867,
        "LSTM Syscall": 0.575347,
        "LSTM Context": 0.915675,
    },
    "original": {
        "STIDE": 0.131434,
        "LSTM Syscall": 1.556929,
        "LSTM Context": 1.498081,
    },
}


@dataclass
class ModelResult:
    model_name: str
    threshold_policy: str
    threshold: float
    trace_name: str
    num_windows: int
    top_k_used: int
    trace_score: float
    prediction: str
    window_scores_df: pd.DataFrame


# =========================
# Device
# =========================
def get_device():
    if torch.cuda.is_available():
        return "cuda"
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


# =========================
# Vocab helpers
# =========================
def load_vocab_payload(vocab_file: Path) -> dict:
    with open(vocab_file, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_token_to_id(payload: dict) -> dict[str, int]:
    if "token_to_id" in payload:
        return {str(k): int(v) for k, v in payload["token_to_id"].items()}
    if "stoi" in payload:
        return {str(k): int(v) for k, v in payload["stoi"].items()}
    raise ValueError(f"Could not find token_to_id/stoi in vocab payload: keys={list(payload.keys())}")


def get_vocab_size(payload: dict) -> int:
    if "vocab_size" in payload:
        return int(payload["vocab_size"])
    return len(extract_token_to_id(payload))


def get_unk_id(token_to_id: dict[str, int]) -> int:
    for key in ["<UNK>", "[UNK]", "UNK", "<unk>", "[unk]", "unk"]:
        if key in token_to_id:
            return int(token_to_id[key])
    return 0


def encode_syscall_sequence(seq_text: str, token_to_id: dict[str, int]) -> list[int]:
    unk_id = get_unk_id(token_to_id)
    tokens = str(seq_text).strip().split()
    return [token_to_id.get(tok, unk_id) for tok in tokens]


# =========================
# Context encoding inference
# =========================
def build_context_candidates():
    return [
        lambda s, p, r: f"{s}|{p}|{r}",
        lambda s, p, r: f"{s}::{p}::{r}",
        lambda s, p, r: f"{s}||{p}||{r}",
        lambda s, p, r: f"{s} {p} {r}",
        lambda s, p, r: f"{s}\t{p}\t{r}",
        lambda s, p, r: f"syscall={s}|process={p}|return={r}",
        lambda s, p, r: f"{s}|proc={p}|ret={r}",
        lambda s, p, r: f"{s}<CTX>{p}<CTX>{r}",
        lambda s, p, r: str((s, p, r)),
        lambda s, p, r: f"({s},{p},{r})",
    ]

def _write_uploaded_raw_trace(uploaded_file, temp_dir: Path) -> Path:
    """
    Save uploaded raw trace as a zip file compatible with process_single_trace.
    Supports:
      - .zip directly
      - .sc by wrapping into a temporary zip
    Returns path to zip file.
    """
    raw_bytes = uploaded_file.getvalue()
    original_name = uploaded_file.name
    suffix = Path(original_name).suffix.lower()
    stem = Path(original_name).stem

    if suffix == ".zip":
        zip_path = temp_dir / original_name
        with open(zip_path, "wb") as f:
            f.write(raw_bytes)
        return zip_path

    if suffix == ".sc":
        zip_path = temp_dir / f"{stem}.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(f"{stem}.sc", raw_bytes)
        return zip_path

    raise ValueError("Raw Trace mode accepts only .zip or .sc files.")


def raw_trace_to_windows_df(uploaded_file) -> pd.DataFrame:
    """
    Use the project's real preprocessing pipeline for raw traces.
    This keeps dashboard inference aligned with training/evaluation preprocessing.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        zip_path = _write_uploaded_raw_trace(uploaded_file, tmp_dir)

        extract_root = tmp_dir / "extracted"
        output_dir = tmp_dir / "processed"

        df = process_single_trace(
            zip_path=str(zip_path),
            split="demo",
            scenario="php_cwe_434",
            extract_root=str(extract_root),
            output_dir=str(output_dir),
            window_size=WINDOW_SIZE,
            stride=STRIDE,
            save_output=False,
        )

        if df is None or df.empty:
            raise ValueError("No usable windows were produced from uploaded raw trace.")

        return df.copy()


def infer_context_formatter(df_windows: pd.DataFrame, token_to_id: dict[str, int]):
    candidates = build_context_candidates()
    best_fn = None
    best_score = -1.0

    sample_df = df_windows.head(30).copy()

    for fn in candidates:
        total = 0
        hits = 0

        for _, row in sample_df.iterrows():
            syscalls = str(row["window_syscalls"]).strip().split()
            procs = str(row["window_process_names"]).strip().split()
            rets = str(row["window_return_status"]).strip().split()

            n = min(len(syscalls), len(procs), len(rets))
            for i in range(n):
                total += 1
                tok = fn(syscalls[i], procs[i], rets[i])
                if tok in token_to_id:
                    hits += 1

        score = hits / total if total > 0 else 0.0
        if score > best_score:
            best_score = score
            best_fn = fn

    return best_fn, best_score


def encode_context_sequence(row: pd.Series, token_to_id: dict[str, int], formatter) -> list[int]:
    unk_id = get_unk_id(token_to_id)

    syscalls = str(row["window_syscalls"]).strip().split()
    procs = str(row["window_process_names"]).strip().split()
    rets = str(row["window_return_status"]).strip().split()

    n = min(len(syscalls), len(procs), len(rets))
    encoded = []
    for i in range(n):
        tok = formatter(syscalls[i], procs[i], rets[i])
        encoded.append(token_to_id.get(tok, unk_id))

    return encoded


# =========================
# Raw trace parsing
# =========================
def map_return_status(result_value: str) -> str:
    if result_value is None or result_value == "":
        return "0"

    try:
        if str(result_value).startswith("-"):
            return "-1"
        if str(result_value) == "0":
            return "0"
        return "1"
    except Exception:
        return "0"


def parse_sc_bytes(file_name: str, raw_bytes: bytes) -> pd.DataFrame:
    rows = []
    trace_name = Path(file_name).stem

    text = raw_bytes.decode("utf-8", errors="ignore")
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        parts = line.split()
        if len(parts) < 7:
            continue

        # Expected pattern (approx):
        # timestamp cpu_id thread_id process_name pid syscall direction res=...
        timestamp = parts[0]
        cpu_id = parts[1]
        thread_id = parts[2]
        process_name = parts[3]
        pid = parts[4]
        syscall_name = parts[5]
        direction = parts[6]

        result_value = None
        for token in parts[7:]:
            if token.startswith("res="):
                result_value = token.split("=", 1)[1]
                break

        # Keep only output/return side for consistency with project pipeline
        if direction != "<":
            continue

        rows.append({
            "trace_name": trace_name,
            "timestamp": timestamp,
            "cpu_id": cpu_id,
            "thread_id": str(thread_id),
            "process_name": process_name,
            "pid": str(pid),
            "syscall_name": syscall_name,
            "return_status": map_return_status(result_value),
        })

    if not rows:
        raise ValueError("No usable output-side syscall rows found in uploaded .sc file.")

    df = pd.DataFrame(rows)
    return df


def parse_zip_bytes(file_name: str, raw_bytes: bytes) -> pd.DataFrame:
    trace_name = Path(file_name).stem

    with zipfile.ZipFile(io.BytesIO(raw_bytes), "r") as zf:
        sc_members = [n for n in zf.namelist() if n.lower().endswith(".sc")]
        if not sc_members:
            raise ValueError("No .sc file found inside uploaded .zip trace.")

        # Use first .sc file
        target = sc_members[0]
        raw_sc = zf.read(target)

    return parse_sc_bytes(trace_name, raw_sc)


def sliding_windows_from_trace_df(trace_df: pd.DataFrame, window_size: int = WINDOW_SIZE, stride: int = STRIDE) -> pd.DataFrame:
    trace_df = trace_df.copy()
    trace_df["timestamp_num"] = pd.to_numeric(trace_df["timestamp"], errors="coerce")
    trace_df = trace_df.dropna(subset=["timestamp_num"])
    trace_df = trace_df.sort_values(["thread_id", "timestamp_num"]).reset_index(drop=True)

    trace_name = trace_df["trace_name"].iloc[0]
    all_rows = []
    global_window_id = 0

    for thread_id, group in trace_df.groupby("thread_id"):
        group = group.sort_values("timestamp_num").reset_index(drop=True)

        if len(group) < window_size:
            continue

        for start_idx in range(0, len(group) - window_size + 1, stride):
            end_idx = start_idx + window_size
            win = group.iloc[start_idx:end_idx]

            all_rows.append({
                "global_window_id": global_window_id,
                "trace_name": trace_name,
                "thread_id": str(thread_id),
                "window_id": global_window_id,
                "window_start_idx": int(start_idx),
                "window_end_idx": int(end_idx - 1),
                "window_syscalls": " ".join(win["syscall_name"].astype(str).tolist()),
                "window_process_names": " ".join(win["process_name"].astype(str).tolist()),
                "window_return_status": " ".join(win["return_status"].astype(str).tolist()),
            })
            global_window_id += 1

    if not all_rows:
        raise ValueError("No windows could be created from uploaded trace.")

    return pd.DataFrame(all_rows)


# =========================
# Prepared CSV handling
# =========================
REQUIRED_CSV_COLS_MIN = {
    "trace_name",
    "window_id",
    "window_start_idx",
    "window_syscalls",
}

REQUIRED_CSV_COLS_CONTEXT = {
    "window_process_names",
    "window_return_status",
}


def load_prepared_csv(file_bytes: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(file_bytes))
    missing = REQUIRED_CSV_COLS_MIN - set(df.columns)
    if missing:
        raise ValueError(f"Prepared CSV missing required columns: {sorted(missing)}")
    return df.copy()


# =========================
# Model loading
# =========================
def load_stide_artifact():
    with open(STIDE_ARTIFACT_FILE, "rb") as f:
        return pickle.load(f)


def load_lstm_model(model_file: Path, vocab_file: Path, device: str):
    vocab_payload = load_vocab_payload(vocab_file)
    vocab_size = get_vocab_size(vocab_payload)

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

    return model, vocab_payload


# =========================
# Scoring helpers
# =========================
def generate_ngrams(tokens, n=6):
    if len(tokens) < n:
        return []
    return [tuple(tokens[i:i+n]) for i in range(len(tokens) - n + 1)]


def stide_window_score(window_syscalls: str, memory_set: set, ngram_size: int) -> float:
    tokens = str(window_syscalls).strip().split()
    ngrams = generate_ngrams(tokens, n=ngram_size)
    if not ngrams:
        return 0.0
    mismatches = sum(1 for ng in ngrams if ng not in memory_set)
    return mismatches / len(ngrams)


class EncodedSeqDataset(Dataset):
    def __init__(self, sequences: list[list[int]]):
        self.inputs = []
        self.targets = []

        for seq in sequences:
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


def score_lstm_sequences(model, encoded_sequences: list[list[int]], device="cpu", batch_size=512):
    import torch.nn.functional as F

    ds = EncodedSeqDataset(encoded_sequences)
    dl = DataLoader(
        ds,
        batch_size=batch_size,
        shuffle=False,
        collate_fn=collate_fn,
        num_workers=0,
        pin_memory=(device == "cuda"),
    )

    all_scores = []

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


def compute_trace_score(window_scores: list[float], top_fraction: float = TOP_FRACTION):
    scores = np.array(window_scores, dtype=float)
    k = max(1, math.ceil(len(scores) * top_fraction))
    top_scores = np.sort(scores)[::-1][:k]
    return float(top_scores.mean()), k


# =========================
# Input parsing entry point
# =========================
def uploaded_file_to_windows_df(uploaded_file, input_mode: str) -> pd.DataFrame:
    if input_mode == "Prepared Trace Windows CSV":
        df = load_prepared_csv(uploaded_file.getvalue())
    else:
        df = raw_trace_to_windows_df(uploaded_file)

    needed = ["trace_name", "window_id", "window_start_idx", "window_syscalls"]
    for col in needed:
        if col not in df.columns:
            raise ValueError(f"Required column missing after parsing: {col}")

    if "window_process_names" not in df.columns:
        df["window_process_names"] = ""
    if "window_return_status" not in df.columns:
        df["window_return_status"] = ""

    return df.copy()


# =========================
# Model-specific analysis
# =========================
def analyze_with_stide(windows_df: pd.DataFrame, threshold_policy: str) -> ModelResult:
    artifact = load_stide_artifact()
    ngram_size = int(artifact["ngram_size"])
    memory_set = artifact["memory_set"]

    work = windows_df.copy()
    work["window_score"] = work["window_syscalls"].apply(lambda x: stide_window_score(x, memory_set, ngram_size))

    trace_score, top_k = compute_trace_score(work["window_score"].tolist())
    threshold = THRESHOLDS[threshold_policy]["STIDE"]
    prediction = "ANOMALY" if trace_score >= threshold else "NORMAL"

    work = work.sort_values("window_score", ascending=False).reset_index(drop=True)

    return ModelResult(
        model_name="STIDE",
        threshold_policy=threshold_policy,
        threshold=threshold,
        trace_name=str(windows_df["trace_name"].iloc[0]),
        num_windows=len(windows_df),
        top_k_used=top_k,
        trace_score=trace_score,
        prediction=prediction,
        window_scores_df=work,
    )


def analyze_with_lstm_syscall(windows_df: pd.DataFrame, device: str, threshold_policy: str) -> ModelResult:
    model, vocab_payload = load_lstm_model(SYSCALL_MODEL_FILE, SYSCALL_VOCAB_FILE, device)
    token_to_id = extract_token_to_id(vocab_payload)

    encoded_sequences = [
        encode_syscall_sequence(seq, token_to_id)
        for seq in windows_df["window_syscalls"].tolist()
    ]

    scores = score_lstm_sequences(model, encoded_sequences, device=device)

    work = windows_df.copy()
    work["window_score"] = scores

    trace_score, top_k = compute_trace_score(scores)
    threshold = THRESHOLDS[threshold_policy]["LSTM Syscall"]
    prediction = "ANOMALY" if trace_score >= threshold else "NORMAL"

    work = work.sort_values("window_score", ascending=False).reset_index(drop=True)

    return ModelResult(
        model_name="LSTM Syscall",
        threshold_policy=threshold_policy,
        threshold=threshold,
        trace_name=str(windows_df["trace_name"].iloc[0]),
        num_windows=len(windows_df),
        top_k_used=top_k,
        trace_score=trace_score,
        prediction=prediction,
        window_scores_df=work,
    )


def analyze_with_lstm_context(windows_df: pd.DataFrame, device: str, threshold_policy: str) -> ModelResult:
    model, vocab_payload = load_lstm_model(CONTEXT_MODEL_FILE, CONTEXT_VOCAB_FILE, device)
    token_to_id = extract_token_to_id(vocab_payload)

    formatter, formatter_score = infer_context_formatter(windows_df, token_to_id)

    if formatter is None:
        raise ValueError("Could not infer context token format from context vocab.")

    encoded_sequences = [
        encode_context_sequence(row, token_to_id, formatter)
        for _, row in windows_df.iterrows()
    ]

    scores = score_lstm_sequences(model, encoded_sequences, device=device)

    work = windows_df.copy()
    work["window_score"] = scores
    work["context_match_score"] = formatter_score

    trace_score, top_k = compute_trace_score(scores)
    threshold = THRESHOLDS[threshold_policy]["LSTM Context"]
    prediction = "ANOMALY" if trace_score >= threshold else "NORMAL"

    work = work.sort_values("window_score", ascending=False).reset_index(drop=True)

    return ModelResult(
        model_name="LSTM Context",
        threshold_policy=threshold_policy,
        threshold=threshold,
        trace_name=str(windows_df["trace_name"].iloc[0]),
        num_windows=len(windows_df),
        top_k_used=top_k,
        trace_score=trace_score,
        prediction=prediction,
        window_scores_df=work,
    )


def run_selected_models(windows_df: pd.DataFrame, selected_models: list[str], threshold_policy: str):
    device = get_device()
    results = []

    if "STIDE" in selected_models:
        results.append(analyze_with_stide(windows_df, threshold_policy))

    if "LSTM Syscall" in selected_models:
        results.append(analyze_with_lstm_syscall(windows_df, device, threshold_policy))

    if "LSTM Context" in selected_models:
        results.append(analyze_with_lstm_context(windows_df, device, threshold_policy))

    return results


def build_comparison_table(results: list[ModelResult]) -> pd.DataFrame:
    rows = []
    for r in results:
        rows.append({
            "Model": r.model_name,
            "Threshold Policy": r.threshold_policy,
            "Threshold": round(r.threshold, 6),
            "Trace Score": round(r.trace_score, 6),
            "Prediction": r.prediction,
            "Num Windows": r.num_windows,
            "Top-k Used": r.top_k_used,
        })
    return pd.DataFrame(rows)


    