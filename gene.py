import pandas as pd
import re


def clean_col_names(df):
    """Cleans column names to be database-friendly."""
    cols = df.columns
    new_cols = []
    for col in cols:
        new_col = col.lower()
        new_col = re.sub(r"[^a-zA-Z0-9_]", "_", new_col)
        new_col = re.sub(r"_+", "_", new_col)
        new_cols.append(new_col)
    df.columns = new_cols
    return df


# --- 1. Load Data ---
try:
    clinical_df = pd.read_csv("GBM_clinical_data.csv")
    survival_df = pd.read_csv("GBM_survival_data.csv")
    expression_df = pd.read_csv("GBM_gene_expression_data.csv")
    print("✅ Data loaded successfully.")
except FileNotFoundError as e:
    print(f"❌ Error: {e}. Please ensure all CSV files are present.")
    exit()

# --- 2. Clean Column Names ---
print("Cleaning column names for database compatibility...")
clinical_df = clean_col_names(clinical_df)
survival_df = clean_col_names(survival_df)
expression_df = clean_col_names(expression_df)
expression_df.rename(
    columns={"sample_id": "sample_id"}, inplace=True
)  # Ensure consistency
print("✅ Column names cleaned.")

# --- 3. Ensure Referential Integrity ---
master_sample_ids = set(clinical_df["sample_id"])
print(f"\nFound {len(master_sample_ids)} unique samples in the master clinical data.")

survival_df_clean = survival_df[survival_df["sample_id"].isin(master_sample_ids)].copy()
expression_df_clean = expression_df[
    expression_df["sample_id"].isin(master_sample_ids)
].copy()
print(f"Survival data: {len(survival_df_clean)} rows kept.")
print(f"Expression data: {len(expression_df_clean)} rows kept.")

# --- 3.5. Convert Data Types ---
print("\nConverting data types...")
# Convert relevant clinical columns to numeric
numeric_clinical_cols = [
    "age_at_initial_pathologic_diagnosis",
    "initial_pathologic_dx_year",
    "birth_days_to",
    "death_days_to",
]
for col in numeric_clinical_cols:
    if col in clinical_df.columns:
        clinical_df[col] = pd.to_numeric(clinical_df[col], errors="coerce")

# Convert all survival data columns (except sample_id) to numeric
for col in survival_df_clean.columns:
    if col != "sample_id":
        survival_df_clean[col] = pd.to_numeric(survival_df_clean[col], errors="coerce")
print("✅ Data types converted.")

# --- 4. Reshape Gene Expression Data ---
print("\nMelting gene expression data...")
expression_long_df = pd.melt(
    expression_df_clean,
    id_vars=["sample_id"],
    var_name="gene_symbol",
    value_name="expression_value",
)
# Ensure the final expression value is numeric
expression_long_df["expression_value"] = pd.to_numeric(
    expression_long_df["expression_value"], errors="coerce"
)
print(f"New long format expression shape: {expression_long_df.shape}")

# --- 5. Export Cleaned Data ---
clinical_df.to_csv("cleaned_clinical_data.csv", index=False)
survival_df_clean.to_csv("cleaned_survival_data.csv", index=False)
expression_long_df.to_csv("cleaned_expression_data.csv", index=False)
print("\n✅ All cleaned data exported to new CSV files.")
