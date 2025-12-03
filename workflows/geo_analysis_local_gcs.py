# geo_analysis_local_gcs.py
import pandas as pd
import geopandas as gpd
from google.cloud import storage
import os

# -----------------------------
# Config
# -----------------------------
DATA_PATH = "data/geo_data_2015_2025.csv"  # ไฟล์ local ของคุณ
GCS_BUCKET = "ชื่อ-bucket-ของคุณ"          # แก้เป็น bucket ของคุณ
OUTPUT_FOLDER = "analysis_results"         # folder ใน bucket

# ----------------------------------
# โหลดไฟล์ local
# ----------------------------------
def load_local_data(path=DATA_PATH):
    if path.endswith(".csv"):
        df = pd.read_csv(path)
    elif path.endswith(".geojson") or path.endswith(".json"):
        df = gpd.read_file(path)
    else:
        raise ValueError("ไฟล์ต้องเป็น CSV หรือ GeoJSON")
    return df

# ----------------------------------
# อัพโหลดไฟล์ไป GCS
# ----------------------------------
def upload_to_gcs(local_path, bucket_name, remote_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(remote_path)
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded {local_path} → gs://{bucket_name}/{remote_path}")

# ----------------------------------
# วิเคราะห์และบันทึกผล
# ----------------------------------
def analyze_and_upload(df):
    print("จำนวนแถวทั้งหมด:", len(df))
    print("5 แถวแรกของข้อมูล:")
    print(df.head())

    # สร้างโฟลเดอร์ temp local
    os.makedirs("temp_output", exist_ok=True)

    # Save CSV
    csv_path = "temp_output/analysis_2015_2025.csv"
    df.to_csv(csv_path, index=False)
    upload_to_gcs(csv_path, GCS_BUCKET, f"{OUTPUT_FOLDER}/analysis_2015_2025.csv")

    # Save GeoJSON ถ้าเป็น GeoDataFrame
    if isinstance(df, gpd.GeoDataFrame):
        geojson_path = "temp_output/analysis_2015_2025.geojson"
        df.to_file(geojson_path, driver="GeoJSON")
        upload_to_gcs(geojson_path, GCS_BUCKET, f"{OUTPUT_FOLDER}/analysis_2015_2025.geojson")

def main():
    df = load_local_data()
    analyze_and_upload(df)

if __name__ == "__main__":
    main()

