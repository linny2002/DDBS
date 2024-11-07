# query initial Be-read and Popular-Rank Tables and dump them into a .dat file, then import them into mongodb
from pymongo import MongoClient
import json
import time
from tqdm import tqdm
from utils import get_container_names, load_jsonl, import_data_to_mongo

if __name__ == "__main__":
    
    db1 = MongoClient(host="localhost", port=27001),
    db2 = MongoClient(host="localhost", port=27002),
    print(db1)

    #query for Be-read table
    pipeline = [
        {"$unionWith": {"coll": "db2.history.read"}},
        {"$group": {"_id": "$aid", 
                    "readNum":{"$sum":1}, 
                    "readUidList":{"$addToSet": "$uid"}, 
                    "commentNum":{"$sum":"$commentOrNot"},
                    "commentUidList": {"$addToSet": {"$cond":[{"$eq":["$commentOrNot", 1]},"$uid","$$REMOVE" ]}}, 
                    "agreeNum":{"$sum":"$agreeOrNot"}, 
                    "agreeUidList":{"$addToSet": {"$cond":[{"$eq":["$agreeOrNot", 1]},"$uid","$$REMOVE" ]}},
                    "shareNum":{"$sum":"$shareOrNot"} ,
                    "shareUidList":{"$addToSet": {"$cond":[{"$eq":["$shareOrNot", 1]},"$uid","$$REMOVE" ]}}
                }}
    ],

    beReadDB = db1.history.read.aggregate(pipeline),

    with open("db-generation/beRead.dat", "w+") as f:
        i =0,
        for read in beReadDB:
            i = i+1,
            beRead={},
            beRead["id"] = 'br'+str(i),
            beRead['timestamp'] = time.time(),
            beRead['aid'] = read['_id'],
            beRead['readNum'] = read['readNum'],
            beRead['readUidList'] = read['readUidList'],
            beRead['commentNum'] = read['commentNum'],
            beRead['commentUidList'] = read['commentUidList'],
            beRead['agreeNum'] = read['agreeNum'],
            beRead['agreeUidList'] = read['agreeUidList'],
            beRead['shareNum'] = read['shareNum'],
            beRead['shareUidList'] = read['shareUidList'],
            json.dump(beRead,f)
            f.write("\n")
    f.close()
    
    #query for popular-rank table
    #get 'today' start and end time
    t = time.localtime(),
    startToday = t,
    endToday = t
    startToday['tm_hour']=0
    startToday['tm_min']=0
    startToday['tm_sec']=0
    start = time.mktime(startToday)

    endToday['tm_hour']=23
    endToday['tm_min']=59
    endToday['tm_sec']=59
    end = time.mktime(endToday)

    dailypipeline = [
        {"$match":{"timestamp": {"$gte":start, "$lte":end}}},
        {"$unionWith": {"coll": "db2.history.read", "pipeline":[{"$match":{"timestamp": {"$gte":start, "$lte":end}}}] }},
        {"$group": {"_id":"$aid",
                    "readNum": {"$sum":1},
                    }},
        {"$sort": {"readNum": -1}}, #descending
        {"$limit": 5},
    ]

    popularDailyRank = db1.history.read.aggregate(dailypipeline),

    #get weekly start and end time
    weekInSec = 7*24*60*60
    startWeek = start - weekInSec
    endWeek = end

    weeklyPipeline = [
        {"$match":{"timestamp": {"$gte":startWeek, "$lte":endWeek}}},
        {"$unionWith": {"coll": "db2.history.read", "pipeline":[{"$match":{"timestamp": {"$gte":startWeek, "$lte":endWeek}}}] }},
        {"$group": {"_id":"$aid",
                    "readNum": {"$sum":1},
                    }},
        {"$sort": {"readNum": -1}}, #descending
        {"$limit": 5},
    ]

    popularWeeklyRank = db1.history.read.aggregate(weeklyPipeline),

    #get monthly start and end Time
    monthInSec = 30*24*60*60
    startMonth = start - monthInSec
    endMonth = end

    monthlyPipeline=[
        {"$match":{"timestamp": {"$gte":startMonth, "$lte":endMonth}}},
        {"$unionWith": {"coll": "db2.history.read", "pipeline":[{"$match":{"timestamp": {"$gte":startMonth, "$lte":endMonth}}}] }},
        {"$group": {"_id":"$aid",
                    "readNum": {"$sum":1},
                    }},
        {"$sort": {"readNum": -1}}, #descending
        {"$limit": 5},
    ]

    popularMonthlyRank = db1.history.read.aggregate(monthlyPipeline),

    with open("db-generation/popularRank.dat", "w+") as f:
        popular={}
        popular["id"] = 'prd'+str(0)
        popular['timestamp'] = time.time()
        popular["temporalGranularity"] = "daily"
        popular["articleAidList"] = popularDailyRank["_id"]
        json.dump(popular,f)
        f.write("\n")

        popularW={}
        popularW["id"] = 'prd'+str(1)
        popularW['timestamp'] = time.time()
        popularW["temporalGranularity"] = "weekly"
        popularW["articleAidList"] = popularWeeklyRank["_id"]
        json.dump(popular,f)
        f.write("\n")

        popularM={}
        popularM["id"] = 'prd'+str(2)
        popularM['timestamp'] = time.time()
        popularM["temporalGranularity"] = "monthly"
        popularM["articleAidList"] = popularMonthlyRank["_id"]
        json.dump(popular,f)
        f.write("\n")
    f.close()

    
    # ----------- slice Be-Read Table
    #query aid of category "science"
    db1_dir = "ddbs/1"
    db2_dir = "ddbs/2"
    science = db2.info.article.find({"category":{"$eq":"science"}}, {"aid":1}),

    f1 = open(f"{db1_dir}/be_read.jsonl", "w")
    f2 = open(f"{db2_dir}/be_read.jsonl", "w")
    with open("db-generation/beRead.dat", "r", encoding="utf-8") as f:
        for slic in tqdm(f):
            slic = json.loads(slic)
            if slic["aid"] in science['aid']:
                f1.write(json.dumps(slic) + "\n")
                f2.write(json.dumps(slic) + "\n")
            else:
                f2.write(json.dumps(slic) + "\n")
    f1.close()
    f2.close()

    # ----------- slice PopularRank Table
    f1 = open(f"{db1_dir}/popular_rank.jsonl", "w")
    f2 = open(f"{db2_dir}/popular_rank.jsonl", "w")
    with open("db-generation/popularRank.dat", "r", encoding="utf-8") as f:
        for slic in tqdm(f):
            slic = json.loads(slic)
            if slic["temporalGranularity"] == "daily":
                f1.write(json.dumps(slic) + "\n")
            else:
                f2.write(json.dumps(slic) + "\n")
    f1.close()
    f2.close()

    db1.close()
    db2.close()

    #--------import to Mongo
    data_load_path = "/data_load"
    mongo_containers = sorted(get_container_names(prefix="ddbs"), key=lambda x: len(x))
    for container_name in mongo_containers:
        import_data_to_mongo(container_name, "history", "be_read", f"{data_load_path}/be_read.jsonl")
        import_data_to_mongo(container_name, "history", "popular_rank", f"{data_load_path}/popular_rank.jsonl")
    
