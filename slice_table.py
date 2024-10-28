# slice the User, Article and Read tables
from tqdm import tqdm
import json
import os
from utils import load_jsonl


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
    with open("db-generation/user.dat", "r", encoding="utf-8") as f:
        for slic in tqdm(f):
            slic = json.loads(slic)
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
    with open("db-generation/article.dat", "r", encoding="utf-8") as f:
        for slic in tqdm(f):
            slic = json.loads(slic)
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
    with open("db-generation/read.dat", "r", encoding="utf-8") as f:
        for slic in tqdm(f):
            slic = json.loads(slic)
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