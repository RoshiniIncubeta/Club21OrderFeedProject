import os
import json
import shutil
import logging
from google.cloud import storage

logger = logging.getLogger(__name__)


def load_env(env_file=".env"):
    if not os.path.exists(env_file):
        return
    
    with open(env_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                
                if value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                elif value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                
                os.environ[key] = value


def save_to_json(data, filename):
    with open(filename, "w") as file:
        json.dump(data, file, indent=4)
        

def post_csv_transform(filename):
    # Read file in binary then try common decodings to avoid platform-specific
    # codec errors (Windows default cp1252). Write back as UTF-8.
    with open(filename, "rb") as bf:
        raw = bf.read()

    text = None
    decodings = ["utf-8", "utf-8-sig", "latin-1", "cp1252"]
    for enc in decodings:
        try:
            text = raw.decode(enc)
            break
        except Exception:
            continue

    if text is None:
        # Last resort: replace invalid characters
        text = raw.decode("utf-8", errors="replace")

    # Cleanup duplicated quotes pattern that sometimes appears
    text = text.replace('""""""', '""')

    # Remove any completely empty lines to prevent extra blank rows
    lines = text.splitlines()
    non_empty_lines = [line for line in lines if line.strip()]
    text = '\n'.join(non_empty_lines)

    # Write normalized UTF-8 file
    with open(filename, "w", encoding="utf-8") as f:
        f.write(text)
        

def remove_dir(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
        logger.info(f"Removed directory: {dir_path}")
    else:
        logger.info(f"Directory does not exist: {dir_path}")
        

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name=bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    logger.info(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.")
    return destination_blob_name


def download_from_gcs(bucket_name, source_blob_name, destination_file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    logger.info(f"Blob {source_blob_name} downloaded to {destination_file_name}.")
    return destination_file_name
