import json
from pathlib import Path

import pandas as pd
import torch
import torch.nn.functional as F
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


def score_window_topk(model, dataloader, device, top_k: int = 3):
    """
    Pencere içindeki her pozisyonda NLL hesapla.
    En yüksek top_k pozisyonun ortalamasını pencere skoru olarak al.
    """
    model.eval()
    scores, labels = [], []

    with torch.no_grad():
        for batch in dataloader:
            input_ids = batch["input_ids"].to(device)
            target_ids = batch["target_ids"].to(device)
            batch_labels = batch["labels"].to(device)

            logits = model(input_ids)
            log_probs = F.log_softmax(logits, dim=-1)

            target_log_probs = log_probs.gather(
                2, target_ids.unsqueeze(-1)
            ).squeeze(-1)                                    # [B, L]

            nll = -target_log_probs                          # [B, L]
            mask = (target_ids != PAD_ID).float()            # [B, L]

            # Mask'lenmiş NLL — pad'lenmiş yerlere -inf yerine 0 vermek için
            # (top-k için pad'lenmiş yerleri çok küçük yap ki seçilmesin)
            masked_nll = nll * mask + (mask - 1) * 1e9       # [B, L]

            # Top-K NLL pozisyonunu bul
            top_k_actual = min(top_k, target_ids.size(1))
            top_values, _ = masked_nll.topk(top_k_actual, dim=1)  # [B, K]
            window_score = top_values.mean(dim=1)            # [B]

            scores.extend(window_score.cpu().tolist())
            labels.extend(batch_labels.cpu().tolist())

    return pd.DataFrame({"label": labels, "score": scores})


if __name__ == "__main__":
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")

    vocab_file = r"data\processed\php_cwe_434\context_vocab.json"
    model_file = r"data\processed\php_cwe_434\lstm_context_sequence_predictor.pt"
    val_csv = r"data\processed\php_cwe_434\validation_encoded_context.csv"

    vocab_size = load_vocab_size(vocab_file)

    dataset = ContextSequencePredictionDataset(
        val_csv,
        max_rows=20_000,
        only_normal=False,
    )

    loader = DataLoader(
        dataset,
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
    print("Model loaded.")

    # 3 farklı top-k'yı dene
    for k in [1, 3, 5]:
        print(f"\n{'=' * 50}")
        print(f"TOP-{k} NLL POOLING")
        print(f"{'=' * 50}")

        df = score_window_topk(model, loader, device, top_k=k)

        normal = df[df["label"] == 0]["score"]
        anomaly = df[df["label"] == 1]["score"]

        print(f"Normal  (n={len(normal)}): mean={normal.mean():.4f}, median={normal.median():.4f}, q90={normal.quantile(0.9):.4f}")
        print(f"Anomaly (n={len(anomaly)}): mean={anomaly.mean():.4f}, median={anomaly.median():.4f}, q90={anomaly.quantile(0.9):.4f}")
        print(f"Anomaly median - Normal median: {anomaly.median() - normal.median():+.4f}")
        print(f"Anomaly q90 - Normal q90: {anomaly.quantile(0.9) - normal.quantile(0.9):+.4f}")

        # Verdict
        if anomaly.median() > normal.quantile(0.9):
            print("✅ İYİ AYRIM")
        elif anomaly.quantile(0.5) > normal.quantile(0.75):
            print("🟡 ORTA AYRIM")
        elif anomaly.mean() > normal.mean() * 1.5:
            print("🟡 MEAN-FARKLI ORTA")
        else:
            print("❌ ZAYIF AYRIM")

        # Save
        output = rf"data\processed\php_cwe_434\diagnostic_scores_top{k}.csv"
        df.to_csv(output, index=False)
        print(f"Saved: {output}")