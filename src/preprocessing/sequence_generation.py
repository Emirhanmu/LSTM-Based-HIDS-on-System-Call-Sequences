from pathlib import Path
import pandas as pd


def generate_windows(sequence, window_size=30, stride=1):
    windows = []
    for i in range(0, len(sequence) - window_size + 1, stride):
        windows.append(sequence[i:i + window_size])
    return windows


def create_thread_sequences(
    df: pd.DataFrame,
    window_size: int = 30,
    stride: int = 1
) -> pd.DataFrame:
    df = df.copy()

    required_columns = [
        "timestamp",
        "thread_id",
        "process_name",
        "syscall",
        "return_status",
    ]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.sort_values("timestamp").reset_index(drop=True)

    grouped = df.groupby("thread_id")

    rows = []
    valid_threads = 0
    total_windows = 0

    for thread_id, group in grouped:
        group = group.sort_values("timestamp").reset_index(drop=True)

        if len(group) < window_size:
            continue

        valid_threads += 1

        timestamps = group["timestamp"].astype(int).tolist()
        syscall_seq = group["syscall"].astype(str).tolist()
        process_seq = group["process_name"].astype(str).tolist()
        status_seq = group["return_status"].astype(int).tolist()

        for idx in range(0, len(group) - window_size + 1, stride):
            end_idx = idx + window_size

            window_timestamps = timestamps[idx:end_idx]
            window_syscalls = syscall_seq[idx:end_idx]
            window_processes = process_seq[idx:end_idx]
            window_statuses = status_seq[idx:end_idx]

            rows.append({
                "thread_id": int(thread_id),
                "window_id": len(rows),
                "window_start_idx": idx,
                "window_end_idx": end_idx - 1,
                "window_start_ts": int(window_timestamps[0]),
                "window_end_ts": int(window_timestamps[-1]),
                "window_syscalls": " ".join(window_syscalls),
                "window_process_names": " ".join(window_processes),
                "window_return_status": " ".join(map(str, window_statuses)),
            })

            total_windows += 1

    windows_df = pd.DataFrame(rows)

    print("Valid threads:", valid_threads)
    print("Total windows:", total_windows)
    print("Output shape:", windows_df.shape)

    return windows_df


def save_windows_csv(df: pd.DataFrame, output_file: str, sep: str = ";"):
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, sep=sep)


if __name__ == "__main__":
    input_file = "data/processed/cleaned_sample.csv"
    output_file = "data/processed/windows_sample.csv"

    if Path(input_file).exists():
        df = pd.read_csv(input_file, sep=";")

        print("Original shape:", df.shape)

        windows_df = create_thread_sequences(df, window_size=30, stride=1)

        print("\nHead:")
        print(windows_df.head())

        save_windows_csv(windows_df, output_file, sep=";")
        print(f"\nSaved windows to: {output_file}")
    else:
        print(f"File not found: {input_file}")