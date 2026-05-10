import shutil
import json
import zipfile
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from src.parsing.parser import parse_file
from src.preprocessing.cleaning import clean_parsed_data
from src.preprocessing.sequence_generation import create_thread_sequences


def extract_trace_zip(zip_path: str, extract_root: str) -> Path:
    zip_path = Path(zip_path)
    extract_root = Path(extract_root)
    target_dir = extract_root / zip_path.stem
    target_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = [
            m for m in zf.namelist()
            if m.endswith(".sc") or m.endswith(".json")
        ]
        for member in members:
            zf.extract(member, target_dir)

    return target_dir


def cleanup_extracted_dir(extracted_dir: Path):
    if extracted_dir.exists():
        shutil.rmtree(extracted_dir, ignore_errors=True)


def find_trace_files(extracted_dir: Path, trace_stem: str) -> Tuple[Path, Path]:
    sc_files = list(extracted_dir.rglob("*.sc"))
    json_files = list(extracted_dir.rglob("*.json"))

    if not sc_files:
        raise FileNotFoundError(f"No .sc file found in {extracted_dir}")
    if not json_files:
        raise FileNotFoundError(f"No .json file found in {extracted_dir}")

    # Önce aynı stem isimli dosyaları tercih et
    sc_path = next((p for p in sc_files if p.stem == trace_stem), sc_files[0])
    json_path = next((p for p in json_files if p.stem == trace_stem), json_files[0])

    return sc_path, json_path


def load_trace_metadata(json_path: str) -> dict:
    with open(json_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    warmup_end_sec = meta.get("time", {}).get("warmup_end", {}).get("absolute")
    exploit_events = meta.get("time", {}).get("exploit", [])

    exploit_time_sec = None
    if exploit_events:
        exploit_time_sec = exploit_events[0].get("absolute")

    return {
        "exploit": bool(meta.get("exploit", False)),
        "exploit_name": meta.get("exploit_name"),
        "image": meta.get("image"),
        "recording_time": meta.get("recording_time"),
        "warmup_end_sec": warmup_end_sec,
        "warmup_end_ns": int(warmup_end_sec * 1_000_000_000) if warmup_end_sec is not None else None,
        "exploit_time_sec": exploit_time_sec,
        "exploit_time_ns": int(exploit_time_sec * 1_000_000_000) if exploit_time_sec is not None else None,
    }


def apply_warmup_filter(cleaned_df: pd.DataFrame, warmup_end_ns: Optional[int]) -> pd.DataFrame:
    if warmup_end_ns is None:
        return cleaned_df.copy()

    df = cleaned_df[cleaned_df["timestamp"] >= warmup_end_ns].copy()
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def assign_window_labels(windows_df: pd.DataFrame, metadata: dict) -> pd.DataFrame:
    if windows_df.empty:
        return windows_df.copy()

    exploit = metadata["exploit"]
    exploit_time_ns = metadata["exploit_time_ns"]

    df = windows_df.copy()

    if not exploit or exploit_time_ns is None:
        df["label"] = 0
        return df

    # exploit var:
    # end < exploit_time -> normal
    # start >= exploit_time -> anomaly
    # arayı kesen pencere -> discard
    normal_df = df[df["window_end_ts"] < exploit_time_ns].copy()
    normal_df["label"] = 0

    anomaly_df = df[df["window_start_ts"] >= exploit_time_ns].copy()
    anomaly_df["label"] = 1

    labeled_df = pd.concat([normal_df, anomaly_df], ignore_index=True)
    labeled_df = labeled_df.sort_values(["thread_id", "window_start_ts"]).reset_index(drop=True)

    return labeled_df


def process_single_trace(
    zip_path: str,
    split: str,
    scenario: str,
    extract_root: str = "data/extracted",
    window_size: int = 30,
    stride: int = 1,
    save_output: bool = False,
    output_dir: str = "data/processed/traces",
) -> pd.DataFrame:
    zip_path = Path(zip_path)
    trace_name = zip_path.stem

    extracted_dir = extract_trace_zip(str(zip_path), extract_root)

    try:
        sc_path, json_path = find_trace_files(extracted_dir, trace_name)

        metadata = load_trace_metadata(str(json_path))

        parsed_df = parse_file(str(sc_path))
        cleaned_df = clean_parsed_data(parsed_df)
        cleaned_df = apply_warmup_filter(cleaned_df, metadata["warmup_end_ns"])

        if cleaned_df.empty:
            return pd.DataFrame()

        windows_df = create_thread_sequences(
            cleaned_df,
            window_size=window_size,
            stride=stride,
        )

        if windows_df.empty:
            return pd.DataFrame()

        windows_df = assign_window_labels(windows_df, metadata)

        if windows_df.empty:
            return pd.DataFrame()

        windows_df["trace_name"] = trace_name
        windows_df["split"] = split
        windows_df["scenario"] = scenario
        windows_df["exploit"] = metadata["exploit"]
        windows_df["exploit_name"] = metadata["exploit_name"]
        windows_df["image"] = metadata["image"]

        ordered_cols = [
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
        windows_df = windows_df[ordered_cols]

        if save_output:
            out_dir = Path(output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{trace_name}_windows.csv"
            windows_df.to_csv(out_file, index=False, sep=";")
            print(f"Saved trace windows to: {out_file}")

        return windows_df

    finally:
        cleanup_extracted_dir(extracted_dir)


if __name__ == "__main__":
    sample_zip = r"data\raw\php_cwe_434\test\normal_and_attack\ancient_keller_8759.zip"

    if Path(sample_zip).exists():
        df = process_single_trace(
            zip_path=sample_zip,
            split="validation",
            scenario="php_cwe_434",
            extract_root="data/extracted/php_cwe_434",
            window_size=30,
            stride=1,
            save_output=True,
            output_dir="data/processed/php_cwe_434/trace_windows",
        )

        if df.empty:
            print("No windows were produced for this trace.")
        else:
            print("\nProcessed shape:", df.shape)
            print("\nLabel counts:")
            print(df["label"].value_counts(dropna=False))
            print("\nHead:")
            print(df.head())
    else:
        print(f"File not found: {sample_zip}")