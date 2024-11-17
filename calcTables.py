# query initial Be-read and Popular-Rank Tables and dump them into a .dat file, then import them into mongodb
from pymongo import MongoClient
import json
import time
from tqdm import tqdm
from utils import get_container_names, load_jsonl, import_data_to_mongo

if __name__ == "__main__":
    
    db1 = MongoClient(host="localhost", port=27001)
    db2 = MongoClient(host="localhost", port=27002)
    print(db1)

    #query for Be-read table
    pipeline = [
        {"$unionWith": {"coll": "db2.history.read"}},
        {"$group": {"_id": "$aid", 
                    "readNum":{"$sum":1}, 
                    "timestamp": {"$addToSet": "$timestamp"},
                    "readUidList":{"$addToSet": "$uid"}, 
                    "commentNum":{"$sum":"$commentOrNot"},
                    "commentUidList": {"$addToSet": {"$cond":[{"$eq":["$commentOrNot", 1]},"$uid","$$REMOVE" ]}}, 
                    "agreeNum":{"$sum":"$agreeOrNot"}, 
                    "agreeUidList":{"$addToSet": {"$cond":[{"$eq":["$agreeOrNot", 1]},"$uid","$$REMOVE" ]}},
                    "shareNum":{"$sum":"$shareOrNot"} ,
                    "shareUidList":{"$addToSet": {"$cond":[{"$eq":["$shareOrNot", 1]},"$uid","$$REMOVE" ]}},
                }
        }
    ]

    beReadDB = db1.history.read.aggregate(pipeline)

    with open("db-generation/beRead.dat", "w") as f:
        i = 0
        for read in beReadDB:
            i = i + 1
            beRead={}
            beRead["id"] = 'br'+str(i)
            beRead['timestamp'] = read['timestamp']
            beRead['aid'] = read['_id']
            beRead['readNum'] = read['readNum']
            beRead['readUidList'] = read['readUidList']
            beRead['commentNum'] = read['commentNum']
            beRead['commentUidList'] = read['commentUidList']
            beRead['agreeNum'] = read['agreeNum']
            beRead['agreeUidList'] = read['agreeUidList']
            beRead['shareNum'] = read['shareNum']
            beRead['shareUidList'] = read['shareUidList']
            json.dump(beRead, f)
            f.write("\n")
    f.close()

    # ----------- slice Be-Read Table
    #query aid of category "science"
    db1_dir = "ddbs/1"
    db2_dir = "ddbs/2"
    sci_cursor = list(db1.info.article.find({}, {"aid": 1}))

    f1 = open(f"{db1_dir}/be_read.jsonl", "w")
    f2 = open(f"{db2_dir}/be_read.jsonl", "w")
    with open("db-generation/beRead.dat", "r", encoding="utf-8") as f:
        for slic in tqdm(f):
            slic = json.loads(slic)
            is_science = False
            for document in sci_cursor:
                if slic["aid"] == document["aid"]:
                    is_science = True
                    f1.write(json.dumps(slic) + "\n")
                    f2.write(json.dumps(slic) + "\n")
                    break
            if not is_science:
                f2.write(json.dumps(slic) + "\n")
    f1.close()
    f2.close()

    #--------import to Mongo
    time.sleep(5)  # in case mounting is not completed
    data_load_path = "/data_load"
    mongo_containers = sorted(get_container_names(prefix="ddbs"), key=lambda x: len(x))
    for container_name in mongo_containers:
        import_data_to_mongo(container_name, "history", "be_read", f"{data_load_path}/be_read.jsonl")
    
    #------query for popular-rank table

    dailypipeline = [
        {"$unwind": "$timestamp"},
        {"$addFields": {"timestampDate": {"$dateFromString": {"dateString": "$timestamp"} } }},
        {"$project": {
            "year": {"$year": "$timestampDate"},
            "month":{"$month": "$timestampDate"},
            "day": {"$dayOfMonth": "$timestampDate"},
            "aid": "$aid",
         }},
        {"$group": {"_id":{"year": "$year", "month": "$month", "day": "$day", "aid": "$aid"},
                    "readNum": {"$sum":1},
                    }},
        {"$sort": {"_id.year": 1, "_id.month":1, "_id.day": 1, "readNum": -1}}, #descending is -1 
        {"$group": {"_id": {"year": "$_id.year", "month": "$_id.month", "day": "$_id.day"},
                    "articles": {"$push": {"aid": "$_id.aid"}}
                    }},
        {"$project": {
            "articleAidList":{"$slice":["articles",5]},
            "date": {"$dateFromParts": {"year":"$_id.year", "month":"$_id.month", "day":"$_id.day"}},
        }},
    ]

    popularDailyRank = db2.history.be_read.aggregate(dailypipeline)

    weeklyPipeline = [
        {"$unwind": "$timestamp"},
        {"$addFields": {"timestampDate": {"$dateFromString": {"dateString": "$timestamp"} } }},
        {"$project": {
            "startWeekDate":{"$dateTrunc": {"date":"$timestampDate", "unit":"week"}},
            "aid": "$aid",
         }},
        {"$group": {"_id":{"startWeekDate": "$startWeekDate", "aid": "$aid"},
                    "readNum": {"$sum":1},
                    }},
        {"$sort": {"_id.startWeekDate": 1, "readNum": -1}}, #descending is -1 
        {"$group": {"_id": {"startWeekDate": "$_id.startWeekDate"},
                    "articles": {"$push": {"aid": "$_id.aid"}}
                    }},
        {"$project": {
            "articleAidList":{"$slice":["articles",5]},
            "date": "$_id.startWeekDate",
        }},
    ]

    popularWeeklyRank = db2.history.be_read.aggregate(weeklyPipeline)

    monthlyPipeline=[
        {"$unwind": "$timestamp"},
        {"$addFields": {"timestampDate": {"$dateFromString": {"dateString": "$timestamp"} } }},
        {"$project": {
            "year": {"$year": "$timestampDate"},
            "month":{"$month": "$timestampDate"},
            "aid": "$aid",
         }},
        {"$group": {"_id":{"year": "$year", "month": "$month", "aid": "$aid"},
                    "readNum": {"$sum":1},
                    }},
        {"$sort": {"_id.year": 1, "_id.month":1, "readNum": -1}}, #descending is -1 
        {"$group": {"_id": {"year": "$_id.year", "month": "$_id.month"},
                    "articles": {"$push": {"aid": "$_id.aid"}}
                    }},
        {"$project": {
            "articleAidList":{"$slice":["articles",5]},
            "date": {"$dateFromParts": {"year":"$_id.year", "month":"$_id.month"}},
        }},
    ]

    popularMonthlyRank = db2.history.be_read.aggregate(monthlyPipeline)

    with open("db-generation/popularRank.dat", "w") as f:
        i = 0
        for daily in popularDailyRank:
            i = i + 1
            popular={}
            popular["id"] = 'prd'+str(i)
            popular['timestamp'] = daily["date"]
            popular["temporalGranularity"] = "daily"
            popular["articleAidList"] = daily["articleAidList"]
            json.dump(popular,f)
            f.write("\n")

        for weekly in popularWeeklyRank:
            i = i + 1
            popularW={}
            popularW["id"] = 'prd'+str(1)
            popularW['timestamp'] = weekly["date"]
            popularW["temporalGranularity"] = "weekly"
            popularW["articleAidList"] = weekly["articleAidList"]
            json.dump(popular,f)
            f.write("\n")

        for monthly in popularMonthlyRank:
            i = i+1
            popularM={}
            popularM["id"] = 'prd'+str(i)
            popularM['timestamp'] = monthly["date"]
            popularM["temporalGranularity"] = "monthly"
            popularM["articleAidList"] = monthly["articleAidList"]
            json.dump(popular,f)
            f.write("\n")
    f.close()

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
        import_data_to_mongo(container_name, "history", "popular_rank", f"{data_load_path}/popular_rank.jsonl")
    
