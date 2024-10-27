# insert the fragmented User, Article and Read tables into mongo databases
import subprocess
from util import get_container_names


def import_data_to_mongo(container_name):
	# The path to our data within docker for both DB1 and DB2 is data_load
	data_load_path = "/data_load"

	subprocess.run(["docker", "exec", "-it", container_name, 
                 "mongoimport", "--db=info", "--collection=user", f"{data_load_path}/user.jsonl"])
	subprocess.run(["docker", "exec", "-it", container_name, 
                 "mongoimport", "--db=info", "--collection=article", f"{data_load_path}/article.jsonl"])
	subprocess.run(["docker", "exec", "-it", container_name, 
                 "mongoimport", "--db=history", "--collection=read", f"{data_load_path}/read.jsonl"])
    # TODO: import Be-Read and Popular-Rank jsonl file to mongo
    # subprocess.run(["docker", "exec", "-it", container_name, 
    #              "mongoimport", "--db=history", "--collection=be_read", f"{data_load_path}/be_read.jsonl"])
    # subprocess.run(["docker", "exec", "-it", container_name, 
    #              "mongoimport", "--db=history", "--collection=popular_rank", f"{data_load_path}/popular_rank.jsonl"])


if __name__ == "__main__":
    mongo_containers = sorted(get_container_names(prefix="ddbs"), key=lambda x: len(x))
    for container in mongo_containers:
        import_data_to_mongo(container)