DB="ddbs1"

docker exec -it $DB mongo --eval "db.getSiblingDB('info').dropDatabase()"
docker exec -it $DB mongo --eval "db.getSiblingDB('history').dropDatabase()"
docker exec -it $DB mongo --eval "db.getSiblingDB('mapping').dropDatabase()"
