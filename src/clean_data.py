import pandas as pd 

#importing henderson csv file 
df_hend = pd.read_csv("../data/raw/Henderson1.csv")
print(df_hend.head)

# Convert percentage and charge columns to numeric (ignoring errors like $ or % symbols)
df_hend["standard_charge|gross"] = pd.to_numeric(df_hend["standard_charge|gross"], errors='coerce')

# Group by description and compute the averages
summary_df = df_hend.groupby("description", as_index=False).agg({
    "standard_charge|gross": "mean"
})

# Rename columns for clarity
summary_df = summary_df.rename(columns={
    "standard_charge|gross": "avg_gross_charge"
})

# Preview
print(summary_df.head(1000))
summary_df.info()

#importing mccook csv file 
df_mc = pd.read_csv("../data/raw/McCook1.csv")
print(df_mc.head)

# Convert percentage and charge columns to numeric (ignoring errors like $ or % symbols)
df_mc["standard_charge|gross"] = pd.to_numeric(df_mc["standard_charge|gross"], errors='coerce')

# Group by description and compute the averages
summary_df_mc = df_mc.groupby("description", as_index=False).agg({
    "standard_charge|gross": "mean"
})

# Rename columns for clarity
summary_df_mc = summary_df_mc.rename(columns={
    "standard_charge|gross": "avg_gross_charge"
})

# Preview
print(summary_df_mc.head(1000))
summary_df_mc.info()


def clean_desc(desc):
    if pd.isna(desc):
        return ""
    return " ".join(desc.lower().strip().split())

summary_df['description_clean'] = summary_df['description'].apply(clean_desc)
summary_df_mc['description_clean'] = summary_df_mc['description'].apply(clean_desc)

merged_df = pd.merge(summary_df, summary_df_mc, on='description_clean')
merged_df.to_csv("../data/cleaned/cleaned_data.csv", index=False)

# each individual 
summary_df_mc.to_csv("../data/cleaned/cleaned_data_mc.csv", index=False)
summary_df.to_csv("../data/cleaned/cleaned_data_hend.csv" , index=False)