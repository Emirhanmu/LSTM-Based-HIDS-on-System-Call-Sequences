from pathlib import Path
from typing import List

import pandas as pd
import torch
from torch.utils.data import Dataset


def parse_encoded_sequence(seq_text: str) -> List[int]:
    if pd.isna(seq_text):
        return []
    return [int(x) for x in str(seq_text).strip().split()]


class NextSyscallPredictionDataset(Dataset):
    """
    encoded_syscalls: örn. 30 uzunluklu token dizisi
    input_ids  = ilk 29 token
    target_id  = son token
    label      = window-level normal/anomaly etiketi (train'de filtre için, val/test'te evaluation için)
    """

    def __init__(
        self,
        csv_path: str,
        max_rows: int | None = None,
        train_only_normal: bool = False,
        min_seq_len: int = 2,
    ):
        self.csv_path = csv_path
        self.df = pd.read_csv(csv_path, sep=";")

        if train_only_normal:
            self.df = self.df[self.df["label"] == 0].copy()

        if max_rows is not None:
            self.df = self.df.iloc[:max_rows].copy()

        self.df = self.df.reset_index(drop=True)

        self.inputs = []
        self.targets = []
        self.labels = []
        self.seq_lens = []

        for _, row in self.df.iterrows():
            seq = parse_encoded_sequence(row["encoded_syscalls"])

            if len(seq) < min_seq_len:
                continue

            input_ids = seq[:-1]
            target_id = seq[-1]

            self.inputs.append(input_ids)
            self.targets.append(target_id)
            self.labels.append(int(row["label"]))
            self.seq_lens.append(len(input_ids))

    def __len__(self):
        return len(self.inputs)

    def __getitem__(self, idx):
        return {
            "input_ids": torch.tensor(self.inputs[idx], dtype=torch.long),
            "target_id": torch.tensor(self.targets[idx], dtype=torch.long),
            "label": torch.tensor(self.labels[idx], dtype=torch.long),
            "seq_len": torch.tensor(self.seq_lens[idx], dtype=torch.long),
        }


def collate_prediction_fn(batch):
    input_ids = [item["input_ids"] for item in batch]
    target_ids = torch.stack([item["target_id"] for item in batch])
    labels = torch.stack([item["label"] for item in batch])
    seq_lens = torch.stack([item["seq_len"] for item in batch])

    padded_inputs = torch.nn.utils.rnn.pad_sequence(
        input_ids,
        batch_first=True,
        padding_value=0,  # PAD token id
    )

    return {
        "input_ids": padded_inputs,
        "target_ids": target_ids,
        "labels": labels,
        "seq_lens": seq_lens,
    }


if __name__ == "__main__":
    train_csv = r"data\processed\php_cwe_434\train_encoded_syscalls.csv"
    val_csv = r"data\processed\php_cwe_434\validation_encoded_syscalls.csv"

    if Path(train_csv).exists():
        train_dataset = NextSyscallPredictionDataset(
            train_csv,
            max_rows=5,
            train_only_normal=True,
        )
        print("Train dataset length:", len(train_dataset))

        for i in range(len(train_dataset)):
            item = train_dataset[i]
            print(f"\nSample {i}")
            print("input_ids shape:", item["input_ids"].shape)
            print("target_id:", item["target_id"].item())
            print("label:", item["label"].item())
            print("seq_len:", item["seq_len"].item())
            print("input_ids first 10:", item["input_ids"][:10])

    else:
        print(f"File not found: {train_csv}")