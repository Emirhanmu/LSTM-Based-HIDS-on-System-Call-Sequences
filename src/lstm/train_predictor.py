import json
from pathlib import Path

import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader

from src.lstm.prediction_dataset import NextSyscallPredictionDataset, collate_prediction_fn
from src.lstm.prediction_model import LSTMNextSyscallPredictor


def load_vocab_size(vocab_file: str) -> int:
    with open(vocab_file, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return int(payload["vocab_size"])


def compute_top1_accuracy(logits: torch.Tensor, targets: torch.Tensor) -> float:
    preds = torch.argmax(logits, dim=1)
    correct = (preds == targets).sum().item()
    total = targets.size(0)
    return correct / total if total > 0 else 0.0


def run_epoch(model, dataloader, criterion, optimizer=None, device="cpu"):
    is_train = optimizer is not None
    model.train() if is_train else model.eval()

    total_loss = 0.0
    total_samples = 0
    total_correct = 0

    for batch in dataloader:
        input_ids = batch["input_ids"].to(device)
        target_ids = batch["target_ids"].to(device)

        if is_train:
            optimizer.zero_grad()

        with torch.set_grad_enabled(is_train):
            logits = model(input_ids)  # [B, vocab_size]
            loss = criterion(logits, target_ids)

            if is_train:
                loss.backward()
                optimizer.step()

        batch_size = input_ids.size(0)
        total_loss += loss.item() * batch_size
        total_samples += batch_size

        preds = torch.argmax(logits, dim=1)
        total_correct += (preds == target_ids).sum().item()

    avg_loss = total_loss / total_samples if total_samples > 0 else 0.0
    accuracy = total_correct / total_samples if total_samples > 0 else 0.0

    return {
        "loss": avg_loss,
        "accuracy": accuracy,
    }


def save_history(history: list[dict], output_file: str):
    df = pd.DataFrame(history)
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, sep=";")
    print(f"Saved predictor training history to: {output_file}")


if __name__ == "__main__":
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")

    vocab_file = r"data\processed\php_cwe_434\syscall_vocab.json"
    train_csv = r"data\processed\php_cwe_434\train_encoded_syscalls.csv"
    val_csv = r"data\processed\php_cwe_434\validation_encoded_syscalls.csv"

    model_output = r"data\processed\php_cwe_434\lstm_predictor.pt"
    history_output = r"data\processed\php_cwe_434\lstm_predictor_train_history.csv"

    vocab_size = load_vocab_size(vocab_file)
    print(f"Vocab size: {vocab_size}")

    # İlk deneme için kontrollü subset
    train_dataset = NextSyscallPredictionDataset(
        train_csv,
        max_rows=100_000,
        train_only_normal=True,
    )

    val_dataset = NextSyscallPredictionDataset(
        val_csv,
        max_rows=20_000,
        train_only_normal=False,
    )

    print(f"Train dataset size: {len(train_dataset)}")
    print(f"Validation dataset size: {len(val_dataset)}")

    train_loader = DataLoader(
        train_dataset,
        batch_size=128,
        shuffle=True,
        collate_fn=collate_prediction_fn,
    )

    val_loader = DataLoader(
        val_dataset,
        batch_size=128,
        shuffle=False,
        collate_fn=collate_prediction_fn,
    )

    model = LSTMNextSyscallPredictor(
        vocab_size=vocab_size,
        embedding_dim=64,
        hidden_dim=128,
        num_layers=1,
        dropout=0.2,
        pad_idx=0,
    ).to(device)

    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)

    num_epochs = 5
    best_val_loss = float("inf")
    history = []

    print("\n====================")
    print("START NEXT-SYSCALL PREDICTOR TRAINING")
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
        print(f"Train loss: {train_metrics['loss']:.4f} | Train acc: {train_metrics['accuracy']:.4f}")
        print(f"Val loss:   {val_metrics['loss']:.4f} | Val acc:   {val_metrics['accuracy']:.4f}")

        history.append({
            "epoch": epoch,
            "train_loss": train_metrics["loss"],
            "train_accuracy": train_metrics["accuracy"],
            "val_loss": val_metrics["loss"],
            "val_accuracy": val_metrics["accuracy"],
        })

        if val_metrics["loss"] < best_val_loss:
            best_val_loss = val_metrics["loss"]
            Path(model_output).parent.mkdir(parents=True, exist_ok=True)
            torch.save(model.state_dict(), model_output)
            print(f"Saved best predictor model to: {model_output}")

    save_history(history, history_output)
    print(f"\nBest validation loss: {best_val_loss:.4f}")