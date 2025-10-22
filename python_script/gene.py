import sys
import re
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

try:
    clinical = pd.read_csv("GBM_clinical_data.csv")
    survival = pd.read_csv("GBM_survival_data.csv")
    expr = pd.read_csv("GBM_gene_expression_data.csv")
except FileNotFoundError as e:
    print(e)
    sys.exit(1)

clinical = clean_cols(clinical)
survival = clean_cols(survival)
expr = clean_cols(expr)

for df in (clinical, survival, expr):
    if "sample_id" in df.columns:
        df["sample_id"] = df["sample_id"].astype(str).str.strip()

ids = set(clinical["sample_id"])
survival = survival[survival["sample_id"].isin(ids)].copy()
expr = expr[expr["sample_id"].isin(ids)].copy()

for col in [
    "age_at_initial_pathologic_diagnosis",
    "initial_pathologic_dx_year",
    "birth_days_to",
    "death_days_to",
]:
    if col in clinical.columns:
        clinical[col] = pd.to_numeric(clinical[col], errors="coerce")

num_cols = [c for c in survival.columns if c != "sample_id"]
survival[num_cols] = survival[num_cols].apply(pd.to_numeric, errors="coerce")

expr_long = expr.melt(
    id_vars="sample_id", var_name="gene_symbol", value_name="expression_value"
)
expr_long["expression_value"] = pd.to_numeric(
    expr_long["expression_value"], errors="coerce"
)

clinical.to_csv("cleaned_clinical_data.csv", index=False)
survival.to_csv("cleaned_survival_data.csv", index=False)
expr_long.to_csv("cleaned_expression_data.csv", index=False)
