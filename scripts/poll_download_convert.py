# poll_download_convert.py
import os
import json
import pandas as pd
from google.cloud import storage

BUCKET_NAME = os.environ["GCS_BUCKET"]
RAW_DIR = "raw_export"
client = storage.Client()

def list_files():
    bucket = client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=RAW_DIR)
    return [b for b in blobs if b.name.endswith(".geojson")]

def safe_flatten_feature(f):
    props = f.get("properties", {})
    props.pop("geometry", None)
    f.pop("geometry", None)
    clean = {}
    for k, v in props.items():
        if isinstance(v, (list, dict)):
            continue
        clean[k] = v
    return clean

def download_and_convert(files):
    bucket = client.bucket(BUCKET_NAME)
    for blob in files:
        print(f"⬇ Downloading {blob.name} ...")
        content = blob.download_as_text()
        data = json.loads(content)
        features = data.get("features", [])
        rows = [safe_flatten_feature(f) for f in features]
        df = pd.DataFrame(rows)

        local_json = blob.name.replace("raw_export/", "local_raw/")
        local_parquet = local_json.replace(".geojson", ".parquet")
        os.makedirs(os.path.dirname(local_json), exist_ok=True)
        os.makedirs(os.path.dirname(local_parquet), exist_ok=True)

        with open(local_json, "w") as f:
            json.dump(rows, f)

        print(f"➡ Converting to {local_parquet}")
        df.to_parquet(local_parquet, index=False)

def main():
    print("📡 Checking Google Cloud Storage...")
    files = list_files()
    if not files:
        print("⚠ No files found in GCS. Maybe export still running.")
        return
    print(f"Found {len(files)} files.")
    download_and_convert(files)
    print("🎉 Conversion complete!")

if __name__ == "__main__":
    main()

