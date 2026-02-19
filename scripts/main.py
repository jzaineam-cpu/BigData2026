import os
from google.cloud import storage

def classify_and_route_file(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    # Solo procesar archivos en raw/
    if not file_name.startswith("raw/"):
        print("Archivo ya procesado. Ignorando.")
        return

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    ext = os.path.splitext(file_name.lower())[1]

    if ext in [".xlsx", ".xls"]:
        new_path = f"processed/excel/{file_name.split('/')[-1]}"
    elif ext in [".jpg", ".jpeg", ".png"]:
        new_path = f"processed/images/{file_name.split('/')[-1]}"
    else:
        new_path = f"processed/other/{file_name.split('/')[-1]}"

    bucket.copy_blob(blob, bucket, new_path)
    bucket.delete_blob(file_name)

    print(f"Archivo movido a {new_path}")
