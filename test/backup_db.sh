docker exec -it ddbs1 bash -c '
DUMP_DIR="/data_load/dump"; 
mkdir -p "$DUMP_DIR";
mongodump --db info --out "$DUMP_DIR"; 
mongodump --db history --out "$DUMP_DIR"; 
mongodump --db mapping --out "$DUMP_DIR"
'