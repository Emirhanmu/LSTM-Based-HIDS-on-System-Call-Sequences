from pathlib import Path
import pandas as pd


def clean_parsed_data(input_file: str, output_file: str):
    df = pd.read_csv(input_file, sep=';')

    print("Columns:", df.columns.tolist())
    print("Original shape:", df.shape)

    df = df.dropna(subset=["timestamp", "thread_id", "process_name", "syscall", "direction"])

    df = df[df["direction"].isin(["<", ">"])]

    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    df["cpu_id"] = pd.to_numeric(df["cpu_id"], errors="coerce")
    df["thread_id"] = pd.to_numeric(df["thread_id"], errors="coerce")
    df["process_id"] = pd.to_numeric(df["process_id"], errors="coerce")
    df["result"] = pd.to_numeric(df["result"], errors="coerce")

    df = df.dropna(subset=["timestamp", "thread_id", "process_id"])

    df["process_name"] = df["process_name"].astype(str).str.strip()
    df["syscall"] = df["syscall"].astype(str).str.strip()

    df["return_status"] = df["result"].apply(
        lambda x: 1 if pd.notna(x) and x >= 0 else (0 if pd.notna(x) and x < 0 else None)
    )

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, sep=';')

    print("Cleaned shape:", df.shape)
    print(f"Saved cleaned file to: {output_file}")

    print("\nUnique process names:", df["process_name"].nunique())
    print("Unique syscalls:", df["syscall"].nunique())
    print("Unique threads:", df["thread_id"].nunique())

    return df


if __name__ == "__main__":
    input_file = "data/processed/parsed_sample.csv"
    output_file = "data/processed/cleaned_sample.csv"

    if Path(input_file).exists():
        df = clean_parsed_data(input_file, output_file)
        print("\nHead:")
        print(df.head())
    else:
        print(f"File not found: {input_file}")