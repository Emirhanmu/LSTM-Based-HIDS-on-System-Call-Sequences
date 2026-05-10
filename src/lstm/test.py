import pandas as pd

df = pd.read_csv("data/processed/php_cwe_434/predictor_validation_scores.csv")

print("Shape:", df.shape)
print()
print("Columns:", df.columns.tolist())
print()
print("First 5 rows:")
print(df.head())