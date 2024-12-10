# Directory where dumps are stored
DUMP_DIR="/data_load/dump"

# Restore the databases
docker exec ddbs1 mongorestore "$DUMP_DIR"
