import sys
import pandas as pd

def clean_cols(df):
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^0-9a-zA-Z]+", "_", regex=True)
        .str.strip("_")
    )
    return df

# 1) Load raw expression data
try:
    raw = pd.read_csv("GBM_gene_expression_data.csv")
except FileNotFoundError as e:
    print(e)
    sys.exit(1)

# 2) Pick first 6 sample IDs
sample_ids = raw["sample_id"].astype(str).str.strip().unique()[:6]

# 3) Filter, clean, reshape
sub = raw[raw["sample_id"].astype(str).str.strip().isin(sample_ids)].copy()
sub = clean_cols(sub)

long = sub.melt(id_vars="sample_id", var_name="gene_symbol", value_name="expression_value")
long["expression_value"] = pd.to_numeric(long["expression_value"], errors="coerce")

# 4) Export
long.to_csv("final_cleaned_expression_2.csv", index=False)
