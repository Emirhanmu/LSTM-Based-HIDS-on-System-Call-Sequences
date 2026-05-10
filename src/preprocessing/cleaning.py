from pathlib import Path
import pandas as pd


def clean_parsed_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Gerekli kolonlar var mı kontrol et
    required_columns = [
        "timestamp",
        "cpu_id",
        "thread_id",
        "process_name",
        "process_id",
        "syscall",
        "direction",
        "result",
    ]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Kritik alanlarda eksik veri varsa at
    df = df.dropna(subset=["timestamp", "thread_id", "process_name", "process_id", "syscall", "direction"])

    # Sadece geçerli direction değerleri
    df = df[df["direction"].isin(["<", ">"])]

    # Sayısal dönüşümler
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    df["cpu_id"] = pd.to_numeric(df["cpu_id"], errors="coerce")
    df["thread_id"] = pd.to_numeric(df["thread_id"], errors="coerce")
    df["process_id"] = pd.to_numeric(df["process_id"], errors="coerce")
    df["result"] = pd.to_numeric(df["result"], errors="coerce")

    # Sayısal dönüşüm sonrası kritik eksikleri at
    df = df.dropna(subset=["timestamp", "thread_id", "process_id"])

    # String kolonları normalize et
    df["process_name"] = df["process_name"].astype(str).str.strip()
    df["syscall"] = df["syscall"].astype(str).str.strip()
    df["direction"] = df["direction"].astype(str).str.strip()

    # return_status üret
    # result >= 0 -> 1
    # result < 0  -> 0
    # result yok  -> -1
    def make_return_status(x):
        if pd.isna(x):
            return -1
        return 1 if x >= 0 else 0

    df["return_status"] = df["result"].apply(make_return_status).astype(int)

    # Final sıralama
    df = df.sort_values("timestamp").reset_index(drop=True)

    return df


def save_cleaned_csv(df: pd.DataFrame, output_file: str, sep: str = ";"):
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, sep=sep)


if __name__ == "__main__":
    input_file = "data/processed/parsed_sample.csv"
    output_file = "data/processed/cleaned_sample.csv"

    if Path(input_file).exists():
        df = pd.read_csv(input_file, sep=";")

        print("Original shape:", df.shape)

        cleaned_df = clean_parsed_data(df)

        print("Cleaned shape:", cleaned_df.shape)
        print("Unique process names:", cleaned_df["process_name"].nunique())
        print("Unique syscalls:", cleaned_df["syscall"].nunique())
        print("Unique threads:", cleaned_df["thread_id"].nunique())
        print("\nHead:")
        print(cleaned_df.head())

        save_cleaned_csv(cleaned_df, output_file, sep=";")
        print(f"\nSaved cleaned file to: {output_file}")
    else:
        print(f"File not found: {input_file}")