# query initial Be-read and Popular-Rank Tables and dump them into a .dat file, then import them into mongodb
from pymongo import MongoClient
import json
import time
import datetime
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
        {"$addFields": {"timestampDate": {"$toDate": {"$convert": {"to": "double", "input": "$timestamp"}}}}},
        {"$project": {
            "year": {"$year": "$timestampDate"},
            "month": {"$month": "$timestampDate"},
            "day": {"$dayOfMonth": "$timestampDate"},
            "aid": "$aid",
        }},
        {"$group": {"_id": {"year": "$year", "month": "$month", "day": "$day", "aid": "$aid"},
                    "readNum": {"$sum": 1},
        }},
        {"$sort": {"_id.year": 1, "_id.month": 1, "_id.day": 1, "readNum": -1}}, #descending is -1 
        {"$group": {"_id": {"year": "$_id.year", "month": "$_id.month", "day": "$_id.day"},
                    "articles": {"$push": {"aid": "$_id.aid"}}
        }},
        {"$project": {
            "articleAidList":{"$slice":["$articles", 5]},
            "date": {"$dateFromParts": {"year": "$_id.year", "month": "$_id.month", "day": "$_id.day"}},
        }},
    ]

    popularDailyRank = db2.history.be_read.aggregate(dailypipeline, allowDiskUse=True)

    weeklyPipeline = [
        # Unwind the timestamp array
        {"$unwind": "$timestamp"},
        
        # Convert the timestamp field to date format
        {"$addFields": {"timestampDate": {"$toDate": {"$convert": {"to": "double", "input": "$timestamp"}}}}},
        
        # Extract year, week, and article ID (aid)
        {"$project": {
            "year": {"$isoWeekYear": "$timestampDate"}, # Extract year (ISO-8601 standard)
            "week": {"$isoWeek": "$timestampDate"},     # Extract week number
            "aid": "$aid"
        }},
        
        # Group by year, week, and aid, and count the number of reads for each article each week
        {"$group": {
            "_id": {"year": "$year", "week": "$week", "aid": "$aid"},
            "readNum": {"$sum": 1}
        }},
        
        # Sort by year and week in ascending order, and within each week, sort by read count in descending order
        {"$sort": {"_id.year": 1, "_id.week": 1, "readNum": -1}},
        
        # Group by year and week, collect the article IDs for the week
        {"$group": {
            "_id": {"year": "$_id.year", "week": "$_id.week"},
            "articles": {"$push": {"aid": "$_id.aid"}}
        }},
        
        # Select the top 5 most-read articles for each week and generate the date range
         {"$project": {
            "articleAidList": {"$slice": ["$articles", 5]}, # Select the top 5 articles
            "weekOfYear": "$_id.week",  # the week number of the year
            "startDate": {"$dateFromParts": {  # Start date of the week
                "isoWeekYear": "$_id.year",
                "isoWeek": "$_id.week",
                "isoDayOfWeek": 1  # First day of the ISO week (Monday)
            }},
            "endDate": {"$dateFromParts": {  # End date of the week
                "isoWeekYear": "$_id.year",
                "isoWeek": "$_id.week",
                "isoDayOfWeek": 7  # Last day of the ISO week (Sunday)
            }}
        }}
    ]

    popularWeeklyRank = db2.history.be_read.aggregate(weeklyPipeline, allowDiskUse=True)

    monthlyPipeline = [
        {"$unwind": "$timestamp"},
        {"$addFields": {"timestampDate": {"$toDate": {"$convert": {"to": "double", "input": "$timestamp"}}}}},
        {"$project": {
            "year": {"$year": "$timestampDate"},  # Extract the year
            "month": {"$month": "$timestampDate"},  # Extract the month
            "aid": "$aid",
        }},
        {"$group": {
            "_id": {"year": "$year", "month": "$month", "aid": "$aid"},
            "readNum": {"$sum": 1}
        }},
        {"$sort": {"_id.year": 1, "_id.month": 1, "readNum": -1}},
        {"$group": {
            "_id": {"year": "$_id.year", "month": "$_id.month"},
            "articles": {"$push": {"aid": "$_id.aid"}}
        }},
        {"$project": {
            "articleAidList": {"$slice": ["$articles", 5]},  # Select the top 5 articles
            "month": "$_id.month",  # the month number of the year
            "startDate": {"$dateFromParts": {  # Start date of the month
                "year": "$_id.year",
                "month": "$_id.month",
                "day": 1
            }},
            "endDate": {"$dateFromParts": {  # End date of the month
                "year": "$_id.year",
                "month": "$_id.month",
                "day": {
                    "$switch": {  # Calculate the last day of the month
                        "branches": [
                            {"case": {"$in": ["$_id.month", [1, 3, 5, 7, 8, 10, 12]]}, "then": 31},
                            {"case": {"$in": ["$_id.month", [4, 6, 9, 11]]}, "then": 30},
                            {"case": {
                                "$and": [
                                    {"$eq": ["$_id.month", 2]},  # Check for February
                                    {"$eq": [{"$mod": ["$_id.year", 4]}, 0]},  # Leap year
                                    {"$or": [
                                        {"$ne": [{"$mod": ["$_id.year", 100]}, 0]},
                                        {"$eq": [{"$mod": ["$_id.year", 400]}, 0]}
                                    ]}
                                ]
                            }, "then": 29},  # February in a leap year
                            {"case": {"$eq": ["$_id.month", 2]}, "then": 28}  # Regular February
                        ],
                        "default": None
                    }
                }
            }}
        }}
    ]

    popularMonthlyRank = db2.history.be_read.aggregate(monthlyPipeline, allowDiskUse=True)
    
    # print(len(list(popularDailyRank)), len(list(popularWeeklyRank)), len(list(popularMonthlyRank)))

    with open("db-generation/popularRank.dat", "w") as f:
        i = 0
        for daily in popularDailyRank:
            i = i + 1
            popularD={}
            popularD["id"] = 'prd'+str(i)
            popularD['timestamp'] = daily["date"].timestamp()
            popularD["temporalGranularity"] = "daily"
            popularD["articleAidList"] = daily["articleAidList"]
            f.write(json.dumps(popularD) + "\n")

        for weekly in popularWeeklyRank:
            i = i + 1
            popularW={}
            popularW["id"] = 'prd'+str(1)
            popularW['timestamp'] = weekly["startDate"].timestamp()
            popularW["temporalGranularity"] = "weekly"
            popularW["articleAidList"] = weekly["articleAidList"]
            f.write(json.dumps(popularW) + "\n")

        for monthly in popularMonthlyRank:
            i = i + 1
            popularM={}
            popularM["id"] = 'prd'+str(i)
            popularM['timestamp'] = monthly["startDate"].timestamp()
            popularM["temporalGranularity"] = "monthly"
            popularM["articleAidList"] = monthly["articleAidList"]
            f.write(json.dumps(popularM) + "\n")
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
    
