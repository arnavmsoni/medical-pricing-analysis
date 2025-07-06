from pathlib import Path
import pandas as pd

SCRIPT_DIR  = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
RAW_DIR      = PROJECT_DIR / "data" / "raw"
CLEAN_DIR    = PROJECT_DIR / "data" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

def process_hospital(filename):
    file_path = RAW_DIR / filename
    print(f"\nLoading {file_path}")

    header_row = None
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f):
            if line.lower().startswith("description,"):
                header_row = i
                break
    if header_row is None:
        raise RuntimeError(f"Could not find header row in {file_path}")

    df = pd.read_csv(
        file_path,
        header=header_row,
        dtype=str,
        na_values=["", "NA"]
    )

    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"\s+", "_", regex=True)
    )
    print("Columns found:", df.columns.tolist())

    gross_cols = [c for c in df.columns if "gross" in c]
    if not gross_cols:
        raise RuntimeError("No column with 'gross' in its name!")
    gross_col = gross_cols[0]
    print(f"→ using '{gross_col}' as the gross charge column")

    df[gross_col] = (
        df[gross_col]
          .str.replace(r"[\$,]", "", regex=True)
          .astype(float)
    )

    summary = (
        df
        .dropna(subset=[gross_col])
        .groupby("description", as_index=False)
        .agg(avg_gross_charge=(gross_col, "mean"))
    )
    print(summary.head(), "\n")
    summary.info()
    return summary

summary_hend = process_hospital("Henderson1.csv")
summary_mc   = process_hospital("McCook1.csv")

def clean_desc(s):
    return "" if pd.isna(s) else " ".join(s.lower().split())

for df in (summary_hend, summary_mc):
    df["description_clean"] = df["description"].apply(clean_desc)

merged = pd.merge(
    summary_hend,
    summary_mc,
    on="description_clean",
    how="inner",
    suffixes=("_hend", "_mc")
)

summary_hend.to_csv(CLEAN_DIR / "cleaned_data_hend.csv", index=False)
summary_mc  .to_csv(CLEAN_DIR / "cleaned_data_mc.csv",   index=False)
merged      .to_csv(CLEAN_DIR / "cleaned_data.csv",      index=False)

print(f"\nAll cleaned files written to {CLEAN_DIR}")
