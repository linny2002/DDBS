#!/bin/bash

# Bring down any currently running containers
SECONDS=0
echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0
docker-compose down
echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0

# Create directories
mkdir -p ./ddbs/1
mkdir -p ./ddbs/2

# Start the Docker Compose services in detached mode
docker-compose up -d
echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0

# slice User, Article and Read Table
python3 slice_table.py
echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0

# import all the tables into DB container
python3 import_data_to_mongo.py  
echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0

# # upload all the texts, images and videos of every article to fastdfs
# docker exec -it storage1 bash -c "cp /etc/fdfs/client.conf /etc/fdfs_buffer"
# mv db-generation/articles/client.conf configs/client.conf
# python3 upload_file_to_dfs.py
# echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0

# upload all the texts, images and videos of every article to fastdfs
docker cp upload_file_to_dfs.sh storage1:/etc/fdfs_buffer/
docker exec -it storage1 bash -c "cd /etc/fdfs_buffer/ && bash ./upload_file_to_dfs.sh"
echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0
cp db-generation/articles/mapping_records.jsonl ddbs/1/mapping_records.jsonl
echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0
cp ddbs/1/mapping_records.jsonl ddbs/2/mapping_records.jsonl
echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0

# import mapping records to DB container
python3 import_map_to_mongo.py  
echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0
