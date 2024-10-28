import os
from fdfs_client.client import get_tracker_conf, Fdfs_client


tracker_conf = get_tracker_conf("./configs/client.conf")
client = Fdfs_client(tracker_conf)

BASE_DIR = "./db-generation/articles"
STORAGE_BASE_URL = "http://0.0.0.0:9090"  # Base URL for accessing files from FastDFS storage
MAPPING_FILE = "./backend/mapping_records.txt"


def upload_file(file_path):
    file_name = os.path.basename(file_path)
    res = client.upload_by_filename(file_path)

    # Check if upload was successful
    if res["Status"] == "Upload successed.":
        file_id = res['Remote file_id'].decode()  # bytes type
        file_url = f"{STORAGE_BASE_URL}/{file_id}"
        with open(MAPPING_FILE, "a") as f:
            f.write(f"{file_name} --> {file_url}\n")
    else:
        with open(MAPPING_FILE, "a") as f:
            f.write(f"Error uploading file: {file_name}\n")


if __name__ == "__main__":
    # Create or clear the mapping file
    open(MAPPING_FILE, "w").close()

    # Iterate over each article folder and upload its files
    for dir in os.listdir(BASE_DIR):
        article_path = os.path.join(BASE_DIR, dir)
        for file_name in os.listdir(article_path):
            file_path = os.path.join(article_path, file_name)
            upload_file(file_path)

    print(f"File upload complete. Mapping records stored in {MAPPING_FILE}")

