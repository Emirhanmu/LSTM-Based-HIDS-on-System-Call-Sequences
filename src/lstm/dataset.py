from pathlib import Path
from typing import List

import pandas as pd
import torch
from torch.utils.data import Dataset


def parse_encoded_sequence(seq_text: str) -> List[int]:
    if pd.isna(seq_text):
        return []
    return [int(x) for x in str(seq_text).strip().split()]


class SyscallSequenceDataset(Dataset):
    def __init__(self, csv_path: str, max_rows: int | None = None):
        self.csv_path = csv_path
        self.df = pd.read_csv(csv_path, sep=";")

        if max_rows is not None:
            self.df = self.df.iloc[:max_rows].copy()

        self.df = self.df.reset_index(drop=True)

        self.labels = self.df["label"].astype(int).tolist()
        self.sequences = self.df["encoded_syscalls"].apply(parse_encoded_sequence).tolist()
        self.seq_lens = self.df["seq_len"].astype(int).tolist()

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        sequence = torch.tensor(self.sequences[idx], dtype=torch.long)
        label = torch.tensor(self.labels[idx], dtype=torch.float32)
        seq_len = torch.tensor(self.seq_lens[idx], dtype=torch.long)

        return {
            "input_ids": sequence,
            "label": label,
            "seq_len": seq_len,
        }


def collate_fn(batch):
    input_ids = [item["input_ids"] for item in batch]
    labels = torch.stack([item["label"] for item in batch])
    seq_lens = torch.stack([item["seq_len"] for item in batch])

    padded_inputs = torch.nn.utils.rnn.pad_sequence(
        input_ids,
        batch_first=True,
        padding_value=0,   # PAD token id
    )

    return {
        "input_ids": padded_inputs,
        "labels": labels,
        "seq_lens": seq_lens,
    }


if __name__ == "__main__":
    train_csv = r"data\processed\php_cwe_434\train_encoded_syscalls.csv"
    val_csv = r"data\processed\php_cwe_434\validation_encoded_syscalls.csv"
    test_csv = r"data\processed\php_cwe_434\test_encoded_syscalls.csv"

    if Path(train_csv).exists():
        train_dataset = SyscallSequenceDataset(train_csv, max_rows=5)
        print("Train dataset length:", len(train_dataset))

        for i in range(len(train_dataset)):
            item = train_dataset[i]
            print(f"\nSample {i}")
            print("input_ids shape:", item["input_ids"].shape)
            print("label:", item["label"].item())
            print("seq_len:", item["seq_len"].item())
            print("input_ids:", item["input_ids"][:10])
    else:
        print(f"File not found: {train_csv}")