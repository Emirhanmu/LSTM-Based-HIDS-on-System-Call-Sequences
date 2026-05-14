from pathlib import Path
import pickle
import time

import pandas as pd


BASE_DIR = Path("data") / "processed" / "php_cwe_434"
OUT_DIR = Path("data") / "artifacts"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TRAIN_CSV = BASE_DIR / "train_windows.csv"
ARTIFACT_FILE = OUT_DIR / "stide_memory_6gram.pkl"


def generate_ngrams(tokens, n=6):
    if len(tokens) < n:
        return []
    return [tuple(tokens[i:i+n]) for i in range(len(tokens) - n + 1)]


def main():
    ngram_size = 6
    max_rows = 500_000
    chunksize = 100_000

    memory_set = set()
    rows_seen = 0
    start = time.time()

    print("Building STIDE memory artifact...")

    for chunk in pd.read_csv(TRAIN_CSV, sep=";", usecols=["window_syscalls"], chunksize=chunksize):
        for seq in chunk["window_syscalls"]:
            tokens = str(seq).strip().split()
            memory_set.update(generate_ngrams(tokens, n=ngram_size))

        rows_seen += len(chunk)
        print(f"Processed train windows: {rows_seen:,} | Unique {ngram_size}-grams: {len(memory_set):,}")

        if rows_seen >= max_rows:
            break

    payload = {
        "ngram_size": ngram_size,
        "max_rows": max_rows,
        "num_windows_used": min(rows_seen, max_rows),
        "memory_set": memory_set,
    }

    with open(ARTIFACT_FILE, "wb") as f:
        pickle.dump(payload, f)

    elapsed = time.time() - start
    print(f"\nSaved STIDE artifact to: {ARTIFACT_FILE}")
    print(f"Windows used: {payload['num_windows_used']:,}")
    print(f"Unique ngrams: {len(memory_set):,}")
    print(f"Elapsed: {elapsed/60:.1f} min")


if __name__ == "__main__":
    main()