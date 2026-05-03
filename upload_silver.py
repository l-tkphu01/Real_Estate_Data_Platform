"""Upload clean silver data (with listing_type) to Azure."""
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
import pyarrow.parquet as pq
import io

conn_str = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=strealestatedatalake;"
    "AccountKey=YOUR_ACCOUNT_KEY_HERE;"
    "EndpointSuffix=core.windows.net"
)
client = BlobServiceClient.from_connection_string(conn_str)
container = client.get_container_client("datalake")

# 1. Read and merge ALL local parquet files
print("Step 1: Reading local data...")
folder = "./silver_export"
files = [f for f in os.listdir(folder) if f.endswith(".parquet")]

all_dfs = []
for f in files:
    df = pd.read_parquet(os.path.join(folder, f))
    if len(df) > 0:
        if "listing_type" not in df.columns:
            df["listing_type"] = "UNKNOWN"
        if "mapping_source" not in df.columns:
            df["mapping_source"] = "Legacy (pre-ML)"
        all_dfs.append(df)

merged = pd.concat(all_dfs, ignore_index=True)
merged = merged.drop_duplicates(subset=["property_id"], keep="last")
print(f"  Records: {len(merged)}")
print(f"  Columns: {list(merged.columns)}")
print(f"  listing_type: {merged['listing_type'].value_counts().to_dict()}")

# 2. Save clean file
clean_path = os.path.join(folder, "clean_silver.parquet")
merged.to_parquet(clean_path, index=False)

# 3. Upload to a NEW clean path (avoid directory issues)
target_blob = "silver/silver_clean/part-00000-clean.snappy.parquet"
print(f"\nStep 2: Uploading to {target_blob}...")
with open(clean_path, "rb") as data:
    container.upload_blob(name=target_blob, data=data, overwrite=True)
print("  Done!")

# 4. Verify
print("\nStep 3: Verifying...")
blob_data = container.download_blob(target_blob).readall()
table = pq.read_table(io.BytesIO(blob_data))
print(f"  Columns: {table.column_names}")
print(f"  Rows: {table.num_rows}")
print(f"  listing_type: {'listing_type' in table.column_names}")
print(f"  mapping_source: {'mapping_source' in table.column_names}")
print(f"  Sample: {table.column('listing_type').to_pylist()[:5]}")
print(f"\nPower BI URL: https://strealestatedatalake.dfs.core.windows.net/datalake/silver/silver_clean/")
