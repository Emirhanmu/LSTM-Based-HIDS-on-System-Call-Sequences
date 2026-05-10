import json
from pathlib import Path

import pandas as pd
import torch
import torch.nn as nn
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


def compute_token_accuracy(logits: torch.Tensor, targets: torch.Tensor, pad_id: int = 0) -> float:
    preds = torch.argmax(logits, dim=-1)
    mask = targets != pad_id

    correct = ((preds == targets) & mask).sum().item()
    total = mask.sum().item()

    return correct / total if total > 0 else 0.0


def run_epoch(model, dataloader, criterion, optimizer=None, device="cpu", pad_id: int = 0):
    is_train = optimizer is not None
    model.train() if is_train else model.eval()

    total_loss = 0.0
    total_acc = 0.0
    total_batches = 0

    for batch in dataloader:
        input_ids = batch["input_ids"].to(device)
        target_ids = batch["target_ids"].to(device)

        if is_train:
            optimizer.zero_grad()

        with torch.set_grad_enabled(is_train):
            logits = model(input_ids)  # [B, L, V]

            loss = criterion(
                logits.view(-1, logits.size(-1)),
                target_ids.view(-1),
            )

            if is_train:
                loss.backward()
                optimizer.step()

        acc = compute_token_accuracy(logits, target_ids, pad_id=pad_id)

        total_loss += loss.item()
        total_acc += acc
        total_batches += 1

    avg_loss = total_loss / total_batches if total_batches > 0 else 0.0
    avg_acc = total_acc / total_batches if total_batches > 0 else 0.0

    return {
        "loss": avg_loss,
        "token_accuracy": avg_acc,
    }


def save_history(history: list[dict], output_file: str):
    df = pd.DataFrame(history)
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, sep=";")
    print(f"Saved context predictor training history to: {output_file}")


if __name__ == "__main__":
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")

    vocab_file = r"data\processed\php_cwe_434\context_vocab.json"
    train_csv = r"data\processed\php_cwe_434\train_encoded_context.csv"
    val_csv = r"data\processed\php_cwe_434\validation_encoded_context.csv"

    model_output = r"data\processed\php_cwe_434\lstm_context_sequence_predictor.pt"
    history_output = r"data\processed\php_cwe_434\lstm_context_sequence_predictor_history.csv"

    vocab_size = load_vocab_size(vocab_file)
    print(f"Context vocab size: {vocab_size}")

    train_dataset = ContextSequencePredictionDataset(
        train_csv,
        max_rows=500_000,
        only_normal=True,
    )

    val_dataset = ContextSequencePredictionDataset(
        val_csv,
        max_rows=20_000,
        only_normal=False,
    )

    print(f"Train dataset size: {len(train_dataset)}")
    print(f"Validation dataset size: {len(val_dataset)}")

    train_loader = DataLoader(
        train_dataset,
        batch_size=256,
        shuffle=True,
        collate_fn=collate_context_sequence_prediction_fn,
    )

    val_loader = DataLoader(
        val_dataset,
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

    criterion = nn.CrossEntropyLoss(ignore_index=PAD_ID)
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)

    num_epochs = 10
    best_val_loss = float("inf")
    history = []

    print("\n====================")
    print("START CONTEXT SEQUENCE PREDICTOR TRAINING")
    print("====================")

    for epoch in range(1, num_epochs + 1):
        train_metrics = run_epoch(
            model=model,
            dataloader=train_loader,
            criterion=criterion,
            optimizer=optimizer,
            device=device,
            pad_id=PAD_ID,
        )

        val_metrics = run_epoch(
            model=model,
            dataloader=val_loader,
            criterion=criterion,
            optimizer=None,
            device=device,
            pad_id=PAD_ID,
        )

        print(f"\nEpoch {epoch}/{num_epochs}")
        print(f"Train loss: {train_metrics['loss']:.4f} | Train token acc: {train_metrics['token_accuracy']:.4f}")
        print(f"Val loss:   {val_metrics['loss']:.4f} | Val token acc:   {val_metrics['token_accuracy']:.4f}")

        history.append({
            "epoch": epoch,
            "train_loss": train_metrics["loss"],
            "train_token_accuracy": train_metrics["token_accuracy"],
            "val_loss": val_metrics["loss"],
            "val_token_accuracy": val_metrics["token_accuracy"],
        })

        if val_metrics["loss"] < best_val_loss:
            best_val_loss = val_metrics["loss"]
            Path(model_output).parent.mkdir(parents=True, exist_ok=True)
            torch.save(model.state_dict(), model_output)
            print(f"Saved best context sequence predictor model to: {model_output}")

    save_history(history, history_output)
    print(f"\nBest validation loss: {best_val_loss:.4f}")