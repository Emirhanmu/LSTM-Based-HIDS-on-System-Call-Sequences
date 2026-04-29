import re
from pathlib import Path
import pandas as pd


def parse_line(line: str):
    line = line.strip()
    if not line:
        return None

    parts = line.split()

    # Minimum beklenen yapı:
    # timestamp cpu_id thread_id process_name process_id syscall direction
    if len(parts) < 7:
        return None

    try:
        timestamp = parts[0]
        cpu_id = parts[1]
        thread_id = parts[2]
        process_name = parts[3]
        process_id = parts[4]
        syscall = parts[5]
        direction = parts[6]

        result_match = re.search(r"res=(-?\d+)", line)
        result = int(result_match.group(1)) if result_match else None

        return {
            "timestamp": timestamp,
            "cpu_id": cpu_id,
            "thread_id": thread_id,
            "process_name": process_name,
            "process_id": process_id,
            "syscall": syscall,
            "direction": direction,
            "result": result,
            "raw_line": line,
        }

    except Exception:
        return None


def parse_file(file_path: str):
    rows = []

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parsed = parse_line(line)
            if parsed is not None:
                rows.append(parsed)

    return pd.DataFrame(rows)


if __name__ == "__main__":
    sample_file = "data/raw/sample.sc"
    output_file = "data/processed/parsed_sample.csv"

    if Path(sample_file).exists():
        df = parse_file(sample_file)
        print(df.head())
        print("\nShape:", df.shape)

        Path("data/processed").mkdir(parents=True, exist_ok=True)
        df.to_csv(output_file, index=False, sep=';')

        print(f"\nSaved parsed file to: {output_file}")
    else:
        print(f"File not found: {sample_file}")