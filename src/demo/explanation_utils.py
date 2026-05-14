from pathlib import Path
import pickle
import torch
import torch.nn.functional as F
import pandas as pd

from src.demo.demo_utils import (
    load_lstm_model,
    extract_token_to_id,
    load_vocab_payload,
    encode_syscall_sequence,
    SYSCALL_MODEL_FILE,
    SYSCALL_VOCAB_FILE,
    PAD_ID,
    get_device,
)

from src.demo.demo_utils import (
    load_lstm_model,
    extract_token_to_id,
    get_device,
    infer_context_formatter,
    encode_context_sequence,
    CONTEXT_MODEL_FILE,
    CONTEXT_VOCAB_FILE,
    SYSCALL_MODEL_FILE,
    SYSCALL_VOCAB_FILE,
)

STIDE_ARTIFACT_FILE = Path("data") / "artifacts" / "stide_memory_6gram.pkl"

def invert_vocab(token_to_id: dict[str, int]) -> dict[int, str]:
    return {v: k for k, v in token_to_id.items()}

def explain_lstm_context_window(top_window_row, top_k_positions: int = 3, top_k_predictions: int = 3):
    """
    Explain a suspicious / high-contributing context window by showing which
    context-token transitions contributed most to the window-level anomaly score.
    """
    device = get_device()
    model, vocab_payload = load_lstm_model(CONTEXT_MODEL_FILE, CONTEXT_VOCAB_FILE, device)
    token_to_id = extract_token_to_id(vocab_payload)
    id_to_token = invert_vocab(token_to_id)

    # Infer the same context token formatting used by the context vocab
    formatter, formatter_score = infer_context_formatter(
        pd.DataFrame([top_window_row]),
        token_to_id,
    )

    if formatter is None:
        return {
            "window_reason": "Could not infer the context-token format used by the context model.",
            "important_positions": [],
            "formatter_match_score": 0.0,
        }

    seq = encode_context_sequence(top_window_row, token_to_id, formatter)

    if len(seq) < 3:
        return {
            "window_reason": "Window too short for LSTM context explanation.",
            "important_positions": [],
            "formatter_match_score": formatter_score,
        }

    input_ids = torch.tensor([seq[:-1]], dtype=torch.long).to(device)
    target_ids = torch.tensor([seq[1:]], dtype=torch.long).to(device)

    with torch.no_grad():
        logits = model(input_ids)
        log_probs = F.log_softmax(logits, dim=-1)

        true_token_log_probs = log_probs.gather(
            dim=2,
            index=target_ids.unsqueeze(-1)
        ).squeeze(-1)

        nll = (-true_token_log_probs).squeeze(0)

        top_values, top_indices = torch.topk(nll, k=min(top_k_positions, len(nll)))

    # Raw aligned tokens from the uploaded window
    raw_syscalls = str(top_window_row["window_syscalls"]).strip().split()
    raw_procs = str(top_window_row["window_process_names"]).strip().split()
    raw_rets = str(top_window_row["window_return_status"]).strip().split()

    n = min(len(raw_syscalls), len(raw_procs), len(raw_rets))

    important_positions = []

    for loss_val, pos in zip(top_values.tolist(), top_indices.tolist()):
        if pos + 1 >= n:
            continue

        prev_ctx = f"{raw_syscalls[pos]} | {raw_procs[pos]} | {raw_rets[pos]}"
        next_ctx = f"{raw_syscalls[pos+1]} | {raw_procs[pos+1]} | {raw_rets[pos+1]}"

        pred_log_probs = log_probs[0, pos]
        pred_vals, pred_ids = torch.topk(pred_log_probs, k=top_k_predictions)

        predicted = []
        for lp, idx in zip(pred_vals.tolist(), pred_ids.tolist()):
            predicted.append({
                "predicted_context_token": id_to_token.get(int(idx), f"<id:{idx}>"),
                "log_prob": float(lp),
            })

        important_positions.append({
            "position": int(pos),
            "observed_context_transition": f"{prev_ctx}  ->  {next_ctx}",
            "observed_next_context": next_ctx,
            "token_nll": float(loss_val),
            "top_predicted_next_contexts": predicted,
        })

    if important_positions:
        transition_text = ", ".join(
            f"`{item['observed_context_transition']}`"
            for item in important_positions[:2]
        )

        expectation_text = "; ".join(
            f"after position {item['position']} the model found "
            f"`{', '.join(p['predicted_context_token'] for p in item['top_predicted_next_contexts'])}` more likely"
            for item in important_positions[:2]
        )

        reason = (
            f"This window contributed strongly to the trace score because the context-aware LSTM assigned "
            f"unusually high prediction loss to context transitions such as {transition_text}. "
            f"In particular, {expectation_text}. "
            f"These observed syscall–process–return-status transitions were less consistent with the normal context patterns "
            f"learned during training."
        )
    else:
        reason = (
            "This window contributed strongly to the trace score because the context-aware LSTM assigned "
            "unusually high prediction loss to several context transitions."
        )

    return {
        "window_reason": reason,
        "important_positions": important_positions,
        "formatter_match_score": formatter_score,
    }

    
def explain_lstm_syscall_window(window_syscalls: str, top_k_positions: int = 3, top_k_predictions: int = 3):
    """
    Explain a suspicious syscall window by showing which next-syscall predictions
    contributed most to the window-level anomaly score.
    """
    device = get_device()
    model, vocab_payload = load_lstm_model(SYSCALL_MODEL_FILE, SYSCALL_VOCAB_FILE, device)
    token_to_id = extract_token_to_id(vocab_payload)
    id_to_token = invert_vocab(token_to_id)

    seq = encode_syscall_sequence(window_syscalls, token_to_id)

    if len(seq) < 3:
        return {
            "window_reason": "Window too short for LSTM sequence explanation.",
            "important_positions": [],
        }

    input_ids = torch.tensor([seq[:-1]], dtype=torch.long).to(device)
    target_ids = torch.tensor([seq[1:]], dtype=torch.long).to(device)

    with torch.no_grad():
        logits = model(input_ids)
        log_probs = F.log_softmax(logits, dim=-1)

        true_token_log_probs = log_probs.gather(
            dim=2,
            index=target_ids.unsqueeze(-1)
        ).squeeze(-1)

        nll = (-true_token_log_probs).squeeze(0)

        top_values, top_indices = torch.topk(nll, k=min(top_k_positions, len(nll)))

        important_positions = []
        raw_tokens = str(window_syscalls).strip().split()

        for loss_val, pos in zip(top_values.tolist(), top_indices.tolist()):
            observed_prev = raw_tokens[pos]
            observed_next = raw_tokens[pos + 1]

            pred_log_probs = log_probs[0, pos]
            pred_vals, pred_ids = torch.topk(pred_log_probs, k=top_k_predictions)

            predicted = []
            for lp, idx in zip(pred_vals.tolist(), pred_ids.tolist()):
                predicted.append({
                    "predicted_syscall": id_to_token.get(int(idx), f"<id:{idx}>"),
                    "log_prob": float(lp),
                })

            important_positions.append({
                "position": int(pos),
                "observed_transition": f"{observed_prev} -> {observed_next}",
                "observed_next_syscall": observed_next,
                "token_nll": float(loss_val),
                "top_predicted_next_syscalls": predicted,
            })

    if important_positions:
        transition_text = ", ".join(
            f"`{item['observed_transition']}`"
            for item in important_positions[:3]
        )

        expectation_text = "; ".join(
            f"after `{raw_tokens[item['position']]}` the model found "
            f"`{', '.join(p['predicted_syscall'] for p in item['top_predicted_next_syscalls'])}` more likely"
            for item in important_positions[:2]
        )

        reason = (
            f"This window was considered suspicious because the syscall-only LSTM assigned unusually high "
            f"prediction loss to transitions such as {transition_text}. "
            f"In particular, {expectation_text}. "
            f"These observed next-syscall transitions were less consistent with the normal syscall patterns "
            f"learned during training."
        )
    else:
        reason = (
            "This window was considered suspicious because the syscall-only LSTM assigned unusually high "
            "prediction loss to several next-syscall transitions."
        )

    return {
        "window_reason": reason,
        "important_positions": important_positions,
    }

def load_stide_memory_for_explanation():
    with open(STIDE_ARTIFACT_FILE, "rb") as f:
        payload = pickle.load(f)
    return payload["memory_set"], int(payload["ngram_size"])


def generate_ngrams(tokens, n=6):
    if len(tokens) < n:
        return []
    return [tuple(tokens[i:i+n]) for i in range(len(tokens) - n + 1)]


def explain_stide_window(window_syscalls: str, max_patterns: int = 5):
    """
    Returns a structured explanation for why a STIDE window is suspicious.
    """
    memory_set, ngram_size = load_stide_memory_for_explanation()

    tokens = str(window_syscalls).strip().split()
    ngrams = generate_ngrams(tokens, n=ngram_size)

    if not ngrams:
        return {
            "window_reason": "Window too short for n-gram analysis.",
            "unseen_ngram_count": 0,
            "total_ngrams": 0,
            "mismatch_ratio": 0.0,
            "top_unseen_ngrams": [],
        }

    unseen = [ng for ng in ngrams if ng not in memory_set]
    mismatch_ratio = len(unseen) / len(ngrams)

    counts = {}
    for ng in unseen:
        counts[ng] = counts.get(ng, 0) + 1

    top_unseen = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:max_patterns]
    formatted = [
        {"pattern": " ".join(ng), "count": cnt}
        for ng, cnt in top_unseen
    ]

    if unseen:
        top_patterns_text = ", ".join(
            f"`{' '.join(ng)}`"
            for ng, _ in top_unseen[:3]
        )

        reason = (
            f"This window was marked as suspicious because it contains "
            f"{len(unseen)} unseen syscall {ngram_size}-grams out of {len(ngrams)} total n-grams "
            f"(mismatch ratio = {mismatch_ratio:.4f}). "
            f"The most influential unfamiliar patterns include {top_patterns_text}. "
            f"These short syscall sequences were not observed in the normal STIDE memory."
        )
    else:
        reason = (
            f"All {len(ngrams)} syscall {ngram_size}-grams in this window were already present "
            f"in the normal STIDE memory."
        )

    return {
        "window_reason": reason,
        "unseen_ngram_count": len(unseen),
        "total_ngrams": len(ngrams),
        "mismatch_ratio": mismatch_ratio,
        "top_unseen_ngrams": formatted,
    }