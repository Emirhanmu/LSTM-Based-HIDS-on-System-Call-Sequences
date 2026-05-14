import json
from collections import Counter
from pathlib import Path
from typing import List

import pandas as pd


PAD_TOKEN = "<PAD>"
UNK_TOKEN = "<UNK>"


def tokenize_window(window_text: str) -> List[str]:
    if pd.isna(window_text):
        return []
    return str(window_text).strip().split()


def build_vocab_from_train(
    train_csv: str,
    token_col: str = "window_syscalls",
    min_freq: int = 1,
    max_rows: int | None = 200_000,
    chunksize: int = 10_000,
) -> tuple[dict, Counter]:
    counter = Counter()
    processed = 0

    print("\n====================")
    print("BUILDING SYSCALL VOCAB")
    print("====================")

    for chunk in pd.read_csv(train_csv, sep=";", usecols=[token_col], chunksize=chunksize):
        for window_text in chunk[token_col]:
            tokens = tokenize_window(window_text)
            counter.update(tokens)

            processed += 1
            if max_rows is not None and processed >= max_rows:
                break

        print(f"Processed train windows for vocab: {processed:,} | Unique raw tokens: {len(counter):,}")

        if max_rows is not None and processed >= max_rows:
            break

    vocab = {
        PAD_TOKEN: 0,
        UNK_TOKEN: 1,
    }

    for token in sorted(counter.keys()):
        if counter[token] >= min_freq:
            vocab[token] = len(vocab)

    print(f"\nFinal windows used for vocab: {processed:,}")
    print(f"Final vocab size: {len(vocab):,}")

    return vocab, counter


def save_vocab(vocab: dict, counter: Counter, output_file: str):
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "token_to_id": vocab,
        "vocab_size": len(vocab),
        "token_frequencies": dict(counter),
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Saved vocab to: {output_path}")


def load_vocab(vocab_file: str) -> dict:
    with open(vocab_file, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload["token_to_id"]


def encode_tokens(tokens: List[str], vocab: dict) -> List[int]:
    unk_id = vocab[UNK_TOKEN]
    return [vocab.get(token, unk_id) for token in tokens]


def encode_window_text(window_text: str, vocab: dict) -> List[int]:
    tokens = tokenize_window(window_text)
    return encode_tokens(tokens, vocab)


def encode_dataset_to_csv(
    input_csv: str,
    output_csv: str,
    vocab: dict,
    max_rows: int | None = None,
    chunksize: int = 10_000,
):
    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    first_write = True
    processed = 0

    usecols = [
        "global_window_id",
        "trace_name",
        "scenario",
        "split",
        "label",
        "window_syscalls",
    ]

    print(f"\n====================")
    print(f"ENCODING DATASET: {input_csv}")
    print(f"====================")

    for chunk in pd.read_csv(input_csv, sep=";", usecols=usecols, chunksize=chunksize):
        encoded_sequences = []
        seq_lengths = []

        for window_text in chunk["window_syscalls"]:
            encoded = encode_window_text(window_text, vocab)
            encoded_sequences.append(" ".join(map(str, encoded)))
            seq_lengths.append(len(encoded))

            processed += 1
            if max_rows is not None and processed >= max_rows:
                break

        out_chunk = chunk.iloc[:len(encoded_sequences)].copy()
        out_chunk["encoded_syscalls"] = encoded_sequences
        out_chunk["seq_len"] = seq_lengths

        out_chunk = out_chunk[
            [
                "global_window_id",
                "trace_name",
                "scenario",
                "split",
                "label",
                "seq_len",
                "encoded_syscalls",
            ]
        ]

        out_chunk.to_csv(
            output_path,
            sep=";",
            index=False,
            mode="w" if first_write else "a",
            header=first_write,
        )

        first_write = False
        print(f"Encoded rows: {processed:,}")

        if max_rows is not None and processed >= max_rows:
            break

    print(f"Saved encoded dataset to: {output_path}")


if __name__ == "__main__":
    scenario = "php_cwe_434"

    train_csv = r"data\processed\php_cwe_434\train_windows.csv"
    validation_csv = r"data\processed\php_cwe_434\validation_windows.csv"
    test_csv = r"data\processed\php_cwe_434\test_windows.csv"

    vocab_file = r"data\processed\php_cwe_434\syscall_vocab.json"

    encoded_train_csv = r"data\processed\php_cwe_434\train_encoded_syscalls.csv"
    encoded_validation_csv = r"data\processed\php_cwe_434\validation_encoded_syscalls.csv"
    encoded_test_csv = r"data\processed\php_cwe_434\test_encoded_syscalls.csv"

    
    max_train_rows_for_vocab = 500_000
    max_train_encode_rows = 500_000
    max_validation_encode_rows = None
    max_test_encode_rows = None

    vocab, counter = build_vocab_from_train(
        train_csv=train_csv,
        token_col="window_syscalls",
        min_freq=1,
        max_rows=max_train_rows_for_vocab,
        chunksize=10_000,
    )

    save_vocab(vocab, counter, vocab_file)

    encode_dataset_to_csv(
        input_csv=train_csv,
        output_csv=encoded_train_csv,
        vocab=vocab,
        max_rows=max_train_encode_rows,
        chunksize=10_000,
    )

    encode_dataset_to_csv(
        input_csv=validation_csv,
        output_csv=encoded_validation_csv,
        vocab=vocab,
        max_rows=max_validation_encode_rows,
        chunksize=10_000,
    )

    encode_dataset_to_csv(
        input_csv=test_csv,
        output_csv=encoded_test_csv,
        vocab=vocab,
        max_rows=max_test_encode_rows,
        chunksize=10_000,
    )
