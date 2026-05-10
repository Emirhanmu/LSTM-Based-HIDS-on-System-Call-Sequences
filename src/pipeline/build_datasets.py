from pathlib import Path
import pandas as pd

from src.pipeline.process_split import process_split_folder


def save_dataset(df: pd.DataFrame, output_file: str, sep: str = ";"):
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, sep=sep)
    print(f"Saved dataset to: {output_path}")


def reorder_dataset(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    df["global_window_id"] = df.index

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
    return df[ordered_cols]


def build_php_cwe_434_datasets(
    window_size: int = 30,
    stride: int = 1,
    train_max_files: int | None = 10,
    val_normal_max_files: int | None = 5,
    val_attack_max_files: int | None = 2,
    test_normal_max_files: int | None = 5,
    test_attack_max_files: int | None = 5,
):
    scenario = "php_cwe_434"
    raw_root = Path("data/raw/php_cwe_434")
    extract_root = "data/extracted/php_cwe_434"
    output_root = Path("data/processed/php_cwe_434")

    # -----------------
    # TRAIN (normal only)
    # -----------------
    print("\n====================")
    print("BUILDING TRAIN SET")
    print("====================")

    train_df = process_split_folder(
        folder_path=str(raw_root / "training"),
        split="train",
        scenario=scenario,
        extract_root=extract_root,
        output_dir=str(output_root),
        window_size=window_size,
        stride=stride,
        recursive=False,
        save_individual_traces=False,
        max_files=train_max_files,
        start_index=0,
        save_split_file=False,
    )

    if not train_df.empty:
        train_df = reorder_dataset(train_df)
        save_dataset(train_df, str(output_root / "train_windows.csv"))

    # -----------------
    # VALIDATION = normal + attack
    # -----------------
    print("\n====================")
    print("BUILDING VALIDATION NORMAL PART")
    print("====================")

    val_normal_df = process_split_folder(
        folder_path=str(raw_root / "validation"),
        split="validation",
        scenario=scenario,
        extract_root=extract_root,
        output_dir=str(output_root),
        window_size=window_size,
        stride=stride,
        recursive=False,
        save_individual_traces=False,
        max_files=val_normal_max_files,
        start_index=0,
        save_split_file=False,
    )

    print("\n====================")
    print("BUILDING VALIDATION ATTACK PART")
    print("====================")

    val_attack_df = process_split_folder(
        folder_path=str(raw_root / "test" / "normal_and_attack"),
        split="validation",
        scenario=scenario,
        extract_root=extract_root,
        output_dir=str(output_root),
        window_size=window_size,
        stride=stride,
        recursive=False,
        save_individual_traces=False,
        max_files=val_attack_max_files,
        start_index=0,   # ilk attack trace'ler validation'a gidiyor
        save_split_file=False,
    )

    val_parts = []
    if not val_normal_df.empty:
        val_parts.append(val_normal_df)
    if not val_attack_df.empty:
        val_parts.append(val_attack_df)

    if val_parts:
        val_df = pd.concat(val_parts, ignore_index=True)
        val_df = reorder_dataset(val_df)
        val_df = val_df.sample(frac=1, random_state=42).reset_index(drop=True)
        val_df["global_window_id"] = val_df.index

        print("\n====================")
        print("VALIDATION SUMMARY")
        print("====================")
        print(f"Total validation windows: {len(val_df)}")
        print("Validation label counts:")
        print(val_df["label"].value_counts(dropna=False))

        save_dataset(val_df, str(output_root / "validation_windows.csv"))
    else:
        val_df = pd.DataFrame()

    # -----------------
    # TEST = normal + remaining attack traces
    # -----------------
    print("\n====================")
    print("BUILDING TEST NORMAL PART")
    print("====================")

    test_normal_df = process_split_folder(
        folder_path=str(raw_root / "test" / "normal"),
        split="test",
        scenario=scenario,
        extract_root=extract_root,
        output_dir=str(output_root),
        window_size=window_size,
        stride=stride,
        recursive=False,
        save_individual_traces=False,
        max_files=test_normal_max_files,
        start_index=0,
        save_split_file=False,
    )

    print("\n====================")
    print("BUILDING TEST ATTACK PART")
    print("====================")

    test_attack_df = process_split_folder(
        folder_path=str(raw_root / "test" / "normal_and_attack"),
        split="test",
        scenario=scenario,
        extract_root=extract_root,
        output_dir=str(output_root),
        window_size=window_size,
        stride=stride,
        recursive=False,
        save_individual_traces=False,
        max_files=test_attack_max_files,
        start_index=val_attack_max_files,  # validation'da kullanılan attack trace'leri atla
        save_split_file=False,
    )

    test_parts = []
    if not test_normal_df.empty:
        test_parts.append(test_normal_df)
    if not test_attack_df.empty:
        test_parts.append(test_attack_df)

    if test_parts:
        test_df = pd.concat(test_parts, ignore_index=True)
        test_df = reorder_dataset(test_df)
        test_df = test_df.sample(frac=1, random_state=42).reset_index(drop=True)
        test_df["global_window_id"] = test_df.index
        
        print("\n====================")
        print("TEST SUMMARY")
        print("====================")
        print(f"Total test windows: {len(test_df)}")
        print("Test label counts:")
        print(test_df["label"].value_counts(dropna=False))

        save_dataset(test_df, str(output_root / "test_windows.csv"))
    else:
        test_df = pd.DataFrame()

    return train_df, val_df, test_df


if __name__ == "__main__":
    train_df, val_df, test_df = build_php_cwe_434_datasets(
        window_size=30,
        stride=1,
        train_max_files=10,
        val_normal_max_files=5,
        val_attack_max_files=2,
        test_normal_max_files=5,
        test_attack_max_files=5,
    )

    print("\n====================")
    print("FINAL DATASET STATUS")
    print("====================")
    print(f"Train shape: {train_df.shape if not train_df.empty else (0, 0)}")
    print(f"Validation shape: {val_df.shape if not val_df.empty else (0, 0)}")
    print(f"Test shape: {test_df.shape if not test_df.empty else (0, 0)}")