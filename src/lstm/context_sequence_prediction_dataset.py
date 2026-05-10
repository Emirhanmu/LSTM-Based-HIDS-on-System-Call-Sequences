from pathlib import Path
from typing import List

import pandas as pd
import torch
from torch.utils.data import Dataset


PAD_ID = 0


def parse_encoded_sequence(seq_text: str) -> List[int]:
    if pd.isna(seq_text):
        return []
    return [int(x) for x in str(seq_text).strip().split()]


class ContextSequencePredictionDataset(Dataset):
    """
    encoded_context örn: [c1, c2, ..., c30]

    input_ids  = [c1, c2, ..., c29]
    target_ids = [c2, c3, ..., c30]

    label anomaly evaluation için metadata olarak tutulur.
    """

    def __init__(
        self,
        csv_path: str,
        max_rows: int | None = None,
        only_normal: bool = False,
        min_seq_len: int = 3,
    ):
        self.csv_path = csv_path
        self.df = pd.read_csv(csv_path, sep=";")

        if only_normal:
            self.df = self.df[self.df["label"] == 0].copy()

        if max_rows is not None:
            self.df = self.df.iloc[:max_rows].copy()

        self.df = self.df.reset_index(drop=True)

        self.inputs = []
        self.targets = []
        self.labels = []
        self.seq_lens = []

        for _, row in self.df.iterrows():
            seq = parse_encoded_sequence(row["encoded_context"])

            if len(seq) < min_seq_len:
                continue

            input_ids = seq[:-1]
            target_ids = seq[1:]

            self.inputs.append(input_ids)
            self.targets.append(target_ids)
            self.labels.append(int(row["label"]))
            self.seq_lens.append(len(input_ids))

    def __len__(self):
        return len(self.inputs)

    def __getitem__(self, idx):
        return {
            "input_ids": torch.tensor(self.inputs[idx], dtype=torch.long),
            "target_ids": torch.tensor(self.targets[idx], dtype=torch.long),
            "label": torch.tensor(self.labels[idx], dtype=torch.long),
            "seq_len": torch.tensor(self.seq_lens[idx], dtype=torch.long),
        }


def collate_context_sequence_prediction_fn(batch):
    input_ids = [item["input_ids"] for item in batch]
    target_ids = [item["target_ids"] for item in batch]
    labels = torch.stack([item["label"] for item in batch])
    seq_lens = torch.stack([item["seq_len"] for item in batch])

    padded_inputs = torch.nn.utils.rnn.pad_sequence(
        input_ids,
        batch_first=True,
        padding_value=PAD_ID,
    )

    padded_targets = torch.nn.utils.rnn.pad_sequence(
        target_ids,
        batch_first=True,
        padding_value=PAD_ID,
    )

    return {
        "input_ids": padded_inputs,
        "target_ids": padded_targets,
        "labels": labels,
        "seq_lens": seq_lens,
    }


if __name__ == "__main__":
    train_csv = r"data\processed\php_cwe_434\train_encoded_context.csv"

    if Path(train_csv).exists():
        ds = ContextSequencePredictionDataset(
            train_csv,
            max_rows=5,
            only_normal=True,
        )

        print("Dataset length:", len(ds))

        for i in range(len(ds)):
            item = ds[i]
            print(f"\nSample {i}")
            print("input_ids shape:", item["input_ids"].shape)
            print("target_ids shape:", item["target_ids"].shape)
            print("label:", item["label"].item())
            print("seq_len:", item["seq_len"].item())
            print("input_ids first 10:", item["input_ids"][:10])
            print("target_ids first 10:", item["target_ids"][:10])
    else:
        print(f"File not found: {train_csv}")