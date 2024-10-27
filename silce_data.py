# slice the User, Article and Read tables
from tqdm import tqdm
import json
import os
from util import load_jsonl


# The actual path to the data outside of docker (in docker the path is "data_load")
db1_dir = "ddbs/1"
db2_dir = "ddbs/2"


if __name__ == "__main__":
    if not os.path.exists(db1_dir):
        os.makedirs(db1_dir)
    if not os.path.exists(db2_dir):
        os.makedirs(db2_dir)
    
    # user
    f1 = open(f"{db1_dir}/user.jsonl", "w")
    f2 = open(f"{db2_dir}/user.jsonl", "w")
    db1_user = set()
    db2_user = set()
    for slic in tqdm(load_jsonl("db-generation/user.dat")):
        if slic["region"] == "Beijing":
            f1.write(json.dumps(slic) + "\n")
            db1_user.add(slic["uid"])
        elif slic["region"] == "Hong Kong":
            f2.write(json.dumps(slic) + "\n")
            db2_user.add(slic["uid"])
        else:
            print(slic)
            assert(slic["region"] in ["Beijing", "HongKong"])
    f1.close()
    f2.close()
            
    # article
    f1 = open(f"{db1_dir}/article.jsonl", "w")
    f2 = open(f"{db2_dir}/article.jsonl", "w")
    for slic in tqdm(load_jsonl("db-generation/article.dat")):
        if slic["category"] == "science":
            f1.write(json.dumps(slic) + "\n")
            f2.write(json.dumps(slic) + "\n")
        elif slic["category"] == "technology":
            f2.write(json.dumps(slic) + "\n")
        else:
            print(slic)
            assert(slic["category"] in ["science", "technology"])
    f1.close()
    f2.close()
    
    # read
    f1 = open(f"{db1_dir}/read.jsonl", "w")
    f2 = open(f"{db2_dir}/read.jsonl", "w")
    for slic in tqdm(load_jsonl("db-generation/read.dat")):
        if slic["uid"] in db1_user:
            f1.write(json.dumps(slic) + "\n")
        elif slic["uid"] in db2_user:
            f2.write(json.dumps(slic) + "\n")
        else:
            print(slic)
            assert(slic["uid"] in db1_user or slic["uid"] in db2_user)
    f1.close()
    f2.close()
    
    # TODO: create Be-Read and Popular-Rank jsonl file