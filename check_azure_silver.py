"""Check schema of silver data on Azure cloud."""
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

for prefix in ["silver/real_estate/", "silver/silver_export/"]:
    print(f"\n=== {prefix} ===")
    blobs = list(container.list_blobs(name_starts_with=prefix))
    parquets = [b.name for b in blobs if b.name.endswith(".parquet")]
    print(f"Total parquet files: {len(parquets)}")
    
    if parquets:
        blob_data = container.download_blob(parquets[0]).readall()
        table = pq.read_table(io.BytesIO(blob_data))
        print(f"Sample file: {parquets[0]}")
        print(f"Columns: {table.column_names}")
        has_lt = "listing_type" in table.column_names
        has_ms = "mapping_source" in table.column_names
        print(f"Has listing_type: {has_lt}")
        print(f"Has mapping_source: {has_ms}")
        if has_lt:
            print(f"listing_type values: {table.column('listing_type').to_pylist()[:5]}")
        if has_ms:
            print(f"mapping_source values: {table.column('mapping_source').to_pylist()[:5]}")
