# insert the fragmented User, Article and Read tables into mongo databases
from utils import get_container_names, load_jsonl, import_data_to_mongo
from pymongo import MongoClient
from tqdm import tqdm


# clients = dict(
#     db1 = [
#         MongoClient(host="localhost", port=27001),
#         # MongoClient(host="ddbs1_bak", port=27003),
#     ],
#     db2 = [
#         MongoClient(host="localhost", port=27002),
#         # MongoClient(host="ddbs2_bak", port=27004),
#     ]
#     )


if __name__ == "__main__":
    mongo_containers = sorted(get_container_names(prefix="ddbs"), key=lambda x: len(x))
    for container_name in mongo_containers:
        # The path to our data within docker for both DB1 and DB2 is data_load
        data_load_path = "/data_load"
    
        import_data_to_mongo(container_name, "info", "user", f"{data_load_path}/user.jsonl")
        import_data_to_mongo(container_name, "info", "article", f"{data_load_path}/article.jsonl")
        import_data_to_mongo(container_name, "history", "read", f"{data_load_path}/read.jsonl")

    # # import data by pymongo
    # for id in [1, 2]:
    #     for client in tqdm(clients[f"db{id}"]):
    #         client.info.user.insert_many(load_jsonl(f"ddbs/{id}/user.jsonl"))
    #         client.info.article.insert_many(load_jsonl(f"ddbs/{id}/article.jsonl"))
    #         client.info.read.insert_many(load_jsonl(f"ddbs/{id}/read.jsonl"))