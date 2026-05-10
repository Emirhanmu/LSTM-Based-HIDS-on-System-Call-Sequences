from pathlib import Path
import pandas as pd

from src.pipeline.process_single_trace import process_single_trace


def process_split_folder(
    folder_path: str,
    split: str,
    scenario: str,
    extract_root: str,
    output_dir: str,
    window_size: int = 30,
    stride: int = 1,
    recursive: bool = False,
    save_individual_traces: bool = False,
    max_files: int | None = None,
    start_index: int = 0,
    save_split_file: bool = True,
) -> pd.DataFrame:
    folder = Path(folder_path)

    if not folder.exists():
        raise FileNotFoundError(f"Folder not found: {folder}")

    zip_files = list(folder.rglob("*.zip")) if recursive else list(folder.glob("*.zip"))
    zip_files = sorted(zip_files)

    if start_index < 0:
        start_index = 0

    zip_files = zip_files[start_index:]

    if max_files is not None:
        zip_files = zip_files[:max_files]

    if not zip_files:
        print(f"No zip files found in: {folder}")
        return pd.DataFrame()

    print(f"Found {len(zip_files)} trace zip files in {folder}")

    all_dfs = []
    failed_traces = []

    for idx, zip_file in enumerate(zip_files, start=1):
        print(f"\n[{idx}/{len(zip_files)}] Processing: {zip_file.name}")

        try:
            trace_df = process_single_trace(
                zip_path=str(zip_file),
                split=split,
                scenario=scenario,
                extract_root=extract_root,
                window_size=window_size,
                stride=stride,
                save_output=save_individual_traces,
                output_dir=str(Path(output_dir) / "trace_windows"),
            )

            if trace_df.empty:
                print(f"  -> No usable windows produced for {zip_file.name}")
                continue

            all_dfs.append(trace_df)

            label_counts = trace_df["label"].value_counts(dropna=False).to_dict()
            print(f"  -> Trace windows: {len(trace_df)}")
            print(f"  -> Label counts: {label_counts}")

        except Exception as e:
            print(f"  -> FAILED: {zip_file.name}")
            print(f"     Error: {e}")
            failed_traces.append((zip_file.name, str(e)))

    if not all_dfs:
        print("\nNo trace produced usable windows.")
        return pd.DataFrame()

    split_df = pd.concat(all_dfs, ignore_index=True)

    # window_id her trace içinde ayrı başlıyordu; istersek global hale getirelim
    split_df = split_df.reset_index(drop=True)
    split_df["global_window_id"] = split_df.index

    ordered_cols = [
        "global_window_id",
        "trace_name",
        "scenario",
        "split",
        "label",
        "exploit",
        "exploit_name",
        "image",
        "thread_id",
        "window_id",
        "window_start_idx",
        "window_end_idx",
        "window_start_ts",
        "window_end_ts",
        "window_syscalls",
        "window_process_names",
        "window_return_status",
    ]
    split_df = split_df[ordered_cols]

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("\n===== SPLIT SUMMARY =====")
    print(f"Split: {split}")
    print(f"Scenario: {scenario}")
    print(f"Total traces processed successfully: {len(all_dfs)}")
    print(f"Total windows: {len(split_df)}")
    print("Overall label counts:")
    print(split_df["label"].value_counts(dropna=False))

    if save_split_file:
        output_file = out_dir / f"{split}_windows.csv"
        split_df.to_csv(output_file, index=False, sep=";")
        print(f"Saved split dataset to: {output_file}")

    if failed_traces:
        failed_file = out_dir / f"{split}_failed_traces.csv"
        failed_df = pd.DataFrame(failed_traces, columns=["trace_name", "error"])
        failed_df.to_csv(failed_file, index=False, sep=";")
        print(f"Saved failed trace log to: {failed_file}")

    return split_df


if __name__ == "__main__":
    split_df = process_split_folder(
        folder_path=r"data\raw\php_cwe_434\training",
        split="train",
        scenario="php_cwe_434",
        extract_root=r"data\extracted\php_cwe_434",
        output_dir=r"data\processed\php_cwe_434",
        window_size=30,
        stride=1,
        recursive=False,
        save_individual_traces=False,
        max_files=10,
    )

    if not split_df.empty:
        print("\nHead:")
        print(split_df.head())