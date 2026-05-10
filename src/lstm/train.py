import json
from pathlib import Path

import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader

from src.lstm.dataset import SyscallSequenceDataset, collate_fn
from src.lstm.model import LSTMAnomalyClassifier


def load_vocab_size(vocab_file: str) -> int:
    with open(vocab_file, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return int(payload["vocab_size"])


def compute_metrics_from_logits(logits: torch.Tensor, labels: torch.Tensor, threshold: float = 0.5) -> dict:
    probs = torch.sigmoid(logits)
    preds = (probs >= threshold).long()
    labels = labels.long()

    tp = int(((preds == 1) & (labels == 1)).sum().item())
    tn = int(((preds == 0) & (labels == 0)).sum().item())
    fp = int(((preds == 1) & (labels == 0)).sum().item())
    fn = int(((preds == 0) & (labels == 1)).sum().item())

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    accuracy = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0 else 0.0
    fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0

    return {
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy,
        "fpr": fpr,
    }


def run_epoch(model, dataloader, criterion, optimizer=None, device="cpu"):
    is_train = optimizer is not None
    model.train() if is_train else model.eval()

    total_loss = 0.0
    total_samples = 0

    all_logits = []
    all_labels = []

    for batch in dataloader:
        input_ids = batch["input_ids"].to(device)
        labels = batch["labels"].to(device)

        if is_train:
            optimizer.zero_grad()

        with torch.set_grad_enabled(is_train):
            logits = model(input_ids)
            loss = criterion(logits, labels)

            if is_train:
                loss.backward()
                optimizer.step()

        batch_size = input_ids.size(0)
        total_loss += loss.item() * batch_size
        total_samples += batch_size

        all_logits.append(logits.detach().cpu())
        all_labels.append(labels.detach().cpu())

    avg_loss = total_loss / total_samples if total_samples > 0 else 0.0
    all_logits = torch.cat(all_logits, dim=0)
    all_labels = torch.cat(all_labels, dim=0)

    metrics = compute_metrics_from_logits(all_logits, all_labels, threshold=0.5)
    metrics["loss"] = avg_loss

    return metrics


def save_metrics(history: list[dict], output_file: str):
    df = pd.DataFrame(history)
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, sep=";")
    print(f"Saved training history to: {output_file}")


if __name__ == "__main__":
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")

    scenario = "php_cwe_434"

    vocab_file = r"data\processed\php_cwe_434\syscall_vocab.json"
    train_csv = r"data\processed\php_cwe_434\train_encoded_syscalls.csv"
    val_csv = r"data\processed\php_cwe_434\validation_encoded_syscalls.csv"

    model_output = r"data\processed\php_cwe_434\lstm_model.pt"
    history_output = r"data\processed\php_cwe_434\lstm_train_history.csv"

    vocab_size = load_vocab_size(vocab_file)
    print(f"Vocab size: {vocab_size}")

    # İlk deneme için kontrollü subset
    train_dataset = SyscallSequenceDataset(train_csv, max_rows=50_000)
    val_dataset = SyscallSequenceDataset(val_csv, max_rows=20_000)

    print(f"Train dataset size: {len(train_dataset)}")
    print(f"Validation dataset size: {len(val_dataset)}")

    train_loader = DataLoader(
        train_dataset,
        batch_size=128,
        shuffle=True,
        collate_fn=collate_fn,
    )

    val_loader = DataLoader(
        val_dataset,
        batch_size=128,
        shuffle=False,
        collate_fn=collate_fn,
    )

    model = LSTMAnomalyClassifier(
        vocab_size=vocab_size,
        embedding_dim=64,
        hidden_dim=128,
        num_layers=1,
        dropout=0.2,
        pad_idx=0,
    ).to(device)

    criterion = nn.BCEWithLogitsLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)

    num_epochs = 5
    best_val_f1 = -1.0
    history = []

    print("\n====================")
    print("START TRAINING")
    print("====================")

    for epoch in range(1, num_epochs + 1):
        train_metrics = run_epoch(
            model=model,
            dataloader=train_loader,
            criterion=criterion,
            optimizer=optimizer,
            device=device,
        )

        val_metrics = run_epoch(
            model=model,
            dataloader=val_loader,
            criterion=criterion,
            optimizer=None,
            device=device,
        )

        print(f"\nEpoch {epoch}/{num_epochs}")
        print(f"Train loss: {train_metrics['loss']:.4f} | Train F1: {train_metrics['f1']:.4f} | Train Recall: {train_metrics['recall']:.4f}")
        print(f"Val loss:   {val_metrics['loss']:.4f} | Val F1:   {val_metrics['f1']:.4f} | Val Recall:   {val_metrics['recall']:.4f} | Val Precision: {val_metrics['precision']:.4f} | Val FPR: {val_metrics['fpr']:.4f}")

        history.append({
            "epoch": epoch,
            "train_loss": train_metrics["loss"],
            "train_precision": train_metrics["precision"],
            "train_recall": train_metrics["recall"],
            "train_f1": train_metrics["f1"],
            "train_fpr": train_metrics["fpr"],
            "val_loss": val_metrics["loss"],
            "val_precision": val_metrics["precision"],
            "val_recall": val_metrics["recall"],
            "val_f1": val_metrics["f1"],
            "val_fpr": val_metrics["fpr"],
        })

        if val_metrics["f1"] > best_val_f1:
            best_val_f1 = val_metrics["f1"]
            Path(model_output).parent.mkdir(parents=True, exist_ok=True)
            torch.save(model.state_dict(), model_output)
            print(f"Saved best model to: {model_output}")

    save_metrics(history, history_output)
    print(f"\nBest validation F1: {best_val_f1:.4f}")