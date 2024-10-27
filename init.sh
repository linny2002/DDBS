#!/bin/bash

# Bring down any currently running containers
SECONDS=0
echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0
docker-compose down
echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0

# Create directories
mkdir -p ./ddbs/1
mkdir -p ./ddbs/2

mkdir -p ./dfs/1
mkdir -p ./dfs/2

# Start the Docker Compose services in detached mode
docker-compose up -d
echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0

# Run your Python script
python slice_data.py  # slice User, Article and Read Table
echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0
# sleep 5;
# echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0
python3 import_data_to_mongo.py  # import User, Article and Read Table into DB container
echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0
# docker exec -it python-app bash -c "cd /usr/src/app/ && python3 ./generate_beread.py"
# echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0
# docker exec -it python-app bash -c "cd /usr/src/app/ && python3 ./generate_popular_rank.py"
# echo "Line $LINENO: $(date) - Command took $SECONDS seconds"; SECONDS=0

