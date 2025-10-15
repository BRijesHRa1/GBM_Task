import pandas as pd
import re


def clean_col_names(df):
    """Cleans column names to be database-friendly."""
    cols = df.columns
    new_cols = []
    for col in cols:
        new_col = col.lower()
        if new_col.startswith("_"):
            new_col = new_col[1:]
        new_col = re.sub(r"[^a-zA-Z0-9_]", "_", new_col)
        new_col = re.sub(r"_+", "_", new_col)
        new_cols.append(new_col)
    df.columns = new_cols
    return df


# --- 1. Load the RAW Gene Expression Data ---
try:
    raw_expression_df = pd.read_csv("GBM_gene_expression_data.csv")
    print("✅ Raw gene expression data loaded successfully.")
except FileNotFoundError as e:
    print(
        f"❌ Error: {e}. Please ensure 'random_GBM_gene_expression_data.csv' is present."
    )
    exit()

# --- 2. Select the First 6 Sample IDs from this File ---
sample_ids_to_keep = raw_expression_df["sample_id"].unique()[:6]
print(f"\nSelected the first 6 sample IDs from the file: {sample_ids_to_keep.tolist()}")

# --- 3. Filter, Clean, and Reshape the Data ---
# Filter the raw data to only these 6 samples
expression_subset_df = raw_expression_df[
    raw_expression_df["sample_id"].isin(sample_ids_to_keep)
].copy()
print(f"Filtered expression data to {len(expression_subset_df)} samples.")

# Clean the column names of the subset
expression_df_clean = clean_col_names(expression_subset_df)

# Melt the cleaned subset into the long format
print("Melting the filtered gene expression data...")
expression_long_df = pd.melt(
    expression_df_clean,
    id_vars=["sample_id"],
    var_name="gene_symbol",
    value_name="expression_value",
)
# Ensure expression value is numeric
expression_long_df["expression_value"] = pd.to_numeric(
    expression_long_df["expression_value"], errors="coerce"
)
print(f"Final long format expression shape: {expression_long_df.shape}")

# --- 4. Export the Final, Small Expression File ---
output_filename = "final_cleaned_expression_2.csv"
expression_long_df.to_csv(output_filename, index=False)
print(f"\n✅ Final, small expression data exported to '{output_filename}'.")
