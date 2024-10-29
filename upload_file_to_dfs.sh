#!/bin/bash

# Base directory where article folders are stored inside the container
BASE_DIR="/etc/fdfs_buffer"

# FastDFS client configuration file
FDFS_CLIENT_CONF="/etc/fdfs/client.conf"

# Base URL for accessing files from FastDFS storage
STORAGE_BASE_URL="http://localhost:8888"

# Output file for mapping results
MAPPING_FILE="/etc/fdfs_buffer/mapping_records.jsonl"

# Function to upload a file and record its mapping
upload_file() {
    local file_path=$1
    local file_name=$(basename "$file_path")
    local file_id=$(fdfs_upload_file $FDFS_CLIENT_CONF "$file_path")
    local result=$?

    # if result code is 0, upload is successful
    if [ $result -eq 0 ]; then
        local file_url="${STORAGE_BASE_URL}/${file_id}"
        echo "{\"name\": \"$file_name\", \"path\": \"$file_url\"}" >> $MAPPING_FILE
    else
        echo "Error uploading file: $file_name" >> $MAPPING_FILE
    fi
}

echo "Uploading files.."

# Check if the base directory exists
if [ ! -d "$BASE_DIR" ]; then
    echo "Directory $BASE_DIR does not exist."
    exit 1
fi

# Create or clear the mapping file
> $MAPPING_FILE

# Iterate over each article folder and upload its files
for article_dir in "$BASE_DIR"/article*/; do  # article*/ matches all subdirectories that begin with "article"
    if [ -d "$article_dir" ]; then  # check if article_dir is a directory 
        for file in "$article_dir"/*; do
            if [ -f "$file" ]; then
                upload_file "$file"
            fi
        done
    fi
done

echo "File upload complete. Mapping stored in $MAPPING_FILE"
