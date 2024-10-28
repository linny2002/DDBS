# insert the fragmented User, Article and Read tables into mongo databases
from utils import get_container_names, import_data_to_mongo


if __name__ == "__main__":
    mongo_containers = sorted(get_container_names(prefix="ddbs"), key=lambda x: len(x))
    for container_name in mongo_containers:
        data_load_path = "/data_load"
        import_data_to_mongo(container_name, "mapping", "article", f"{data_load_path}/mapping_records.jsonl")
