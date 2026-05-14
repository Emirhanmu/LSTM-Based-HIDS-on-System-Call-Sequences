from pathlib import Path
import pandas as pd


def load_trace_set(csv_path: Path) -> set[str]:
    df = pd.read_csv(csv_path, sep=";", usecols=["trace_name"])
    return set(df["trace_name"].dropna().unique().tolist())


if __name__ == "__main__":
    base_dir = Path("data") / "processed" / "php_cwe_434"

    train_file = base_dir / "train_windows.csv"
    val_file = base_dir / "validation_windows.csv"
    test_file = base_dir / "test_windows.csv"

    print("Loading unique trace names...")
    train_traces = load_trace_set(train_file)
    val_traces = load_trace_set(val_file)
    test_traces = load_trace_set(test_file)

    print("\n====================")
    print("TRACE COUNTS")
    print("====================")
    print(f"Train traces:      {len(train_traces)}")
    print(f"Validation traces: {len(val_traces)}")
    print(f"Test traces:       {len(test_traces)}")

    train_val_overlap = train_traces & val_traces
    train_test_overlap = train_traces & test_traces
    val_test_overlap = val_traces & test_traces

    print("\n====================")
    print("TRACE OVERLAPS")
    print("====================")
    print(f"Train ∩ Validation: {len(train_val_overlap)}")
    print(f"Train ∩ Test:       {len(train_test_overlap)}")
    print(f"Validation ∩ Test:  {len(val_test_overlap)}")

    if train_val_overlap:
        print("\nTrain ∩ Validation examples:")
        print(sorted(list(train_val_overlap))[:20])

    if train_test_overlap:
        print("\nTrain ∩ Test examples:")
        print(sorted(list(train_test_overlap))[:20])

    if val_test_overlap:
        print("\nValidation ∩ Test examples:")
        print(sorted(list(val_test_overlap))[:20])

    if not train_val_overlap and not train_test_overlap and not val_test_overlap:
        print("\n✅ No trace-name overlap detected across splits.")