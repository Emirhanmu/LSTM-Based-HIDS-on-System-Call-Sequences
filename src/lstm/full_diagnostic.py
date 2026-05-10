import json
from pathlib import Path

import pandas as pd
import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader

# Senin mevcut kodlarını birebir kullanıyoruz
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


def score_window_aggregate(model, dataloader, device):
    """
    Pencere boyunca ORTALAMA negative log-likelihood.
    Tek-token değil — pencerenin tüm pozisyonlarında tahmin alınıp ortalama.
    """
    model.eval()
    scores, labels = [], []

    with torch.no_grad():
        for batch in dataloader:
            input_ids = batch["input_ids"].to(device)        # [B, L]
            target_ids = batch["target_ids"].to(device)      # [B, L]
            batch_labels = batch["labels"].to(device)        # [B]

            logits = model(input_ids)                        # [B, L, V]
            log_probs = F.log_softmax(logits, dim=-1)       # [B, L, V]

            # Her pozisyondaki gerçek hedef token'ın log-prob'u
            target_log_probs = log_probs.gather(
                2, target_ids.unsqueeze(-1)
            ).squeeze(-1)                                    # [B, L]

            # Padding maskelemesi (pad_id != target olan pozisyonlar)
            mask = (target_ids != PAD_ID).float()            # [B, L]

            # Pencere başına ortalama NLL
            nll_sum = -(target_log_probs * mask).sum(dim=1)  # [B]
            valid_lens = mask.sum(dim=1).clamp(min=1)        # [B]
            window_score = nll_sum / valid_lens              # [B]

            scores.extend(window_score.cpu().tolist())
            labels.extend(batch_labels.cpu().tolist())

    return pd.DataFrame({"label": labels, "score": scores})


if __name__ == "__main__":
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")

    vocab_file = r"data\processed\php_cwe_434\context_vocab.json"
    model_file = r"data\processed\php_cwe_434\lstm_context_sequence_predictor.pt"
    val_csv = r"data\processed\php_cwe_434\validation_encoded_context.csv"
    output_csv = r"data\processed\php_cwe_434\diagnostic_scores.csv"

    vocab_size = load_vocab_size(vocab_file)
    print(f"Vocab size: {vocab_size}")

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

    print("\nScoring with WINDOW-AGGREGATE NLL...")
    df = score_window_aggregate(model, loader, device)

    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    print(f"Saved scores to: {output_csv}")
    print(f"Scores shape: {df.shape}")

    # =====================================================
    # SCORE DAĞILIMI
    # =====================================================
    print("\n" + "=" * 50)
    print("SCORE DISTRIBUTION BY LABEL")
    print("=" * 50)

    for label_val in [0, 1]:
        sub = df[df["label"] == label_val]["score"]
        name = "NORMAL " if label_val == 0 else "ANOMALY"
        print(f"\n{name} (n={len(sub)}):")
        print(f"  mean   = {sub.mean():.4f}")
        print(f"  median = {sub.median():.4f}")
        print(f"  std    = {sub.std():.4f}")
        print(f"  min    = {sub.min():.4f}")
        print(f"  q10    = {sub.quantile(0.10):.4f}")
        print(f"  q25    = {sub.quantile(0.25):.4f}")
        print(f"  q75    = {sub.quantile(0.75):.4f}")
        print(f"  q90    = {sub.quantile(0.90):.4f}")
        print(f"  max    = {sub.max():.4f}")

    # =====================================================
    # AYRILABILIRLIK ANALIZI
    # =====================================================
    print("\n" + "=" * 50)
    print("SEPARABILITY ANALYSIS")
    print("=" * 50)

    normal = df[df["label"] == 0]["score"]
    anomaly = df[df["label"] == 1]["score"]

    print(f"Normal mean:    {normal.mean():.4f}")
    print(f"Anomaly mean:   {anomaly.mean():.4f}")
    print(f"Mean difference: {anomaly.mean() - normal.mean():.4f}")
    print()
    print(f"Normal q75:     {normal.quantile(0.75):.4f}")
    print(f"Anomaly q25:    {anomaly.quantile(0.25):.4f}")
    print()
    print(f"Normal q90:     {normal.quantile(0.90):.4f}")
    print(f"Anomaly q50:    {anomaly.quantile(0.50):.4f}")

    print("\nVERDICT:")
    if anomaly.quantile(0.50) > normal.quantile(0.90):
        print("✅ İYİ: Anomaly median > Normal q90.")
        print("   Model anomaly'leri ayırt ediyor — sorun threshold seçiminde.")
    elif anomaly.quantile(0.25) > normal.quantile(0.75):
        print("🟡 ORTA: Az örtüşme var ama dağılımlar büyük ölçüde ayrı.")
        print("   FPR-constrained threshold ile iyi sonuç alınabilir.")
    elif anomaly.mean() > normal.mean() * 1.3:
        print("🟡 ZAYIF AYRIM: Mean'lar farklı ama dağılımlar büyük ölçüde örtüşüyor.")
        print("   Model bir şeyler öğrenmiş ama yetersiz.")
    elif anomaly.mean() > normal.mean():
        print("❌ ÇOK ZAYIF: Mean'lar yakın. Model anomaly'leri zar zor ayırıyor.")
        print("   Multi-stream model gerekli.")
    else:
        print("🚨 KRİTİK: Anomaly mean <= Normal mean. Score yönü ters veya bug var.")