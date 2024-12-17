from flask import Flask, request, render_template
import requests
from pymongo import MongoClient
import time
from datetime import datetime, timezone, timedelta


clients = dict(
    db1 = [
        MongoClient(host="localhost", port=27001),
        # MongoClient(host="ddbs1_bak", port=27003),
    ],
    db2 = [
        MongoClient(host="localhost", port=27002),
        # MongoClient(host="ddbs2_bak", port=27004),
    ]
    )
app = Flask(__name__)
ITEM_PER_PAGE = 40

db1_user_count, db2_user_count = 0, 0
for client in clients["db1"]:
    try:
        db1_user_count = client.info.user.count_documents({})
        break
    except:
        pass
for client in clients["db2"]:
    try:
        db2_user_count = client.info.user.count_documents({})
        break
    except:
        pass
user_list_page_num = (db1_user_count + db2_user_count) // ITEM_PER_PAGE
if (db1_user_count + db2_user_count) % ITEM_PER_PAGE != 0:
    user_list_page_num += 1

db2_article_count = 0
for client in clients["db2"]:
    try:
        db2_article_count = client.info.article.count_documents({})
        break
    except:
        pass
article_list_page_num = db2_article_count // ITEM_PER_PAGE
if db2_article_count % ITEM_PER_PAGE != 0:
    article_list_page_num += 1


# text, image and video files of an article is stored in the dfs
def find_file_path(file_name):
    for client in sum(list(clients.values()), []):
        # try:
            res = client.mapping.article.find_one({"name": file_name})
            return res["path"]
        # except:
        #     pass
    return None


def user_by_id(uid):
    for client in sum(list(clients.values()), []):
        try:
            user = client.info.user.find_one({"uid": uid})
            if user:
                return user
        except:
            pass
    return {"message": f"User {uid} not found."}


def article_by_id(aid):
    for client in clients["db2"]:
        # try:
            article = client.info.article.find_one({"aid": aid})
            beRead = client.history.be_read.find_one({"aid": aid})
            text_file = article["text"]
            text_path = find_file_path(text_file).strip()
            text = requests.get(text_path).text
            images = [find_file_path(i).strip() for i in article["image"].split(",") if i.strip()]
            videos = [find_file_path(i).strip() for i in article["video"].split(",") if i.strip()]
            article["timestamp"] = int(article["timestamp"])/1000
            article["date"] = time.strftime("%Y-%m-%d %H:%M", time.localtime(int(article["timestamp"])))
 
    comments = []
    for client in sum(list(clients.values()), []):
        commentDetailList = client.history.read.find({"$and":[{"uid": {"$in": beRead["commentUidList"]}},{"aid": aid}]})
        for comment in commentDetailList:
            #print(comment["commentDetail"])
            comments.append(comment["commentDetail"])
         
    commentDetailList =dict(commentDetailList)
    #print(images)
    #print(comments)
    #del commentDetailList["id"], commentDetailList["timestamp"], commentDetailList["readTimeLength"]
    del article["_id"], article["timestamp"], article["text"], article["image"], article["video"], beRead["id"], beRead["aid"]
    ret = dict(text=text, images=images, videos=videos, **article, **beRead, comments=comments)
    return ret
        # except:
        #     pass
        

def find_user_read_list(uid):
    for client in sum(list(clients.values()), []):
        try:
            history = list(client.history.read.find({"uid": uid}))
            if history:
                return history
        except:
            pass
    return []    

def get_popular_by_granularity(granularity, date):
    date = float(date)
    if granularity=="daily":
        for client in clients["db1"]:
            try:
                popular = client.history.popular_rank.find_one({"timestamp": date})
                if popular:
                    return popular
            except:
                pass
    else:
        for client in clients["db2"]:
            try:
                popular = client.history.popular_rank.find_one({"$and":[{"temporalGranularity": granularity},{"timestamp": date}]})
                if popular:
                    return popular
            except:
                pass
    date = time.strftime("%Y-%m-%d" , time.localtime(date))
    return {"message": f"Top5 for {date} not found. Articles are from around late September 2017 to mid January 2018."}


@app.route("/frontend/user_list/", methods=["GET"])
def get_user_list_page():
    pageid = request.args.get("pageid")
    return user_list_page(pageid)
    

@app.route("/frontend/user_list/<pageid>")
def user_list_page(pageid: int):
    pageid = int(pageid)  # [(pageid-1)*ITEM_PER_PAGE+1, pageid*ITEM_PER_PAGE]
    if not pageid:
        pageid = 1
    
    le, ri = (pageid - 1) * ITEM_PER_PAGE + 1, pageid * ITEM_PER_PAGE
    if ri <= db1_user_count:
        skipnum = le - 1
        for client in clients["db1"]:
            # try:
                res = client.info.user.find().skip(skipnum).limit(ITEM_PER_PAGE)
                user_list = list(res)
                break
            # except:
            #     pass
    elif le > db1_user_count:
        skipnum = le - 1 - db1_user_count
        for client in clients["db2"]:
            # try:
                res = client.info.user.find().skip(skipnum).limit(ITEM_PER_PAGE)
                user_list = list(res)
                break
            # except:
            #     pass
    else:  # le <= db1_user_count < ri
        skipnum = le - 1
        user_list = []
        for client in clients["db1"]:
            # try:
                res = client.info.user.find().skip(skipnum)
                user_list = list(res)
                break
            # except:
            #     pass
        for client in clients["db2"]:
            # try:
                res = client.info.user.find().limit(ri - db1_user_count)
                user_list.extend(list(res))
                break
            # except:
            #     pass
    return render_template("user_list.html", pageid=pageid, user_list=user_list, item_per_page=ITEM_PER_PAGE, total_page_num=user_list_page_num)


@app.route("/frontend/article_list/", methods=["GET"])
def get_article_list_page():
    pageid = request.args.get("pageid")
    return article_list_page(pageid)
    

@app.route("/frontend/article_list/<pageid>")
def article_list_page(pageid: int):
    pageid = int(pageid)
    if not pageid:
        pageid = 1
        
    for client in clients["db2"]:
        # try:  # in case one of the db brokedown
            res = client.info.article.find().skip((pageid - 1) * ITEM_PER_PAGE).limit(ITEM_PER_PAGE)
            article_list = list(res)
            for article in article_list:
                article["cover"] = find_file_path(article["image"].split(",")[0])
                article["date"] = time.strftime("%Y-%m-%d %H:%M", time.localtime(int(article["timestamp"]) / 1000))
                del article["_id"]
            break
        # except:
        #     pass
    return render_template("article_list.html", pageid=pageid, article_list=article_list, item_per_page=ITEM_PER_PAGE, total_page_num=article_list_page_num)


@app.route("/frontend/search_user/", methods=["GET"])
def get_search_user_results():
    pageid = request.args.get("pageid")
    if not pageid: 
        pageid = 1
    search_text = request.args.get("search_text")
    return search_user_results(search_text, pageid)
    
    
@app.route("/frontend/search_user/<search_text>/<pageid>")
def search_user_results(search_text: str, pageid: str):
    pageid = int(pageid)
    fields = ["uid", "name", "email", "phone"]  # 需要搜索的字段
    query = {
        "$or": [
            {field: {"$regex": search_text, "$options": "i"}}
            for field in fields
        ]
    }
    searched_user_num = {
        "db1": 0,
        "db2": 0
    }
    user_list = []
    for client_name in ["db1", "db2"]:
        for client in clients[client_name]:
            # try:
                searched_user_num[client_name] = client.info.user.count_documents(query)
                break
    le, ri = (pageid - 1) * ITEM_PER_PAGE + 1, pageid * ITEM_PER_PAGE
    if ri <= searched_user_num["db1"]:
        skipnum = le - 1
        for client in clients["db1"]:
            # try:
                res = client.info.user.find(query).skip(skipnum).limit(ITEM_PER_PAGE)
                user_list = list(res)
                break
            # except:
            #     pass
    elif le > searched_user_num["db1"]:
        skipnum = le - 1 - searched_user_num["db1"]
        for client in clients["db2"]:
            # try:
                res = client.info.user.find(query).skip(skipnum).limit(ITEM_PER_PAGE)
                user_list = list(res)
                break
            # except:
            #     pass
    else:  # le <= db1_user_count < ri
        skipnum = le - 1
        user_list = []
        for client in clients["db1"]:
            # try:
                res = client.info.user.find(query).skip(skipnum)
                user_list = list(res)
                break
            # except:
            #     pass
        for client in clients["db2"]:
            # try:
                res = client.info.user.find(query).limit(ri - searched_user_num["db1"])
                user_list.extend(list(res))
                break
            # except:
            #     pass
    total_user_num = searched_user_num["db1"] + searched_user_num["db2"]
    total_page_num = total_user_num // ITEM_PER_PAGE + 1
    return render_template("search_user_results.html", pageid=pageid, user_list=user_list, item_per_page=ITEM_PER_PAGE, total_page_num=total_page_num, last_search_text=search_text, total_user_num=total_user_num)


@app.route("/frontend/search_article/", methods=["GET"])
def get_search_article_results():
    pageid = request.args.get("pageid")
    if not pageid: 
        pageid = 1
    search_text = request.args.get("search_text")
    return search_article_results(search_text, pageid)
    
    
@app.route("/frontend/search_article/<search_text>/<pageid>")
def search_article_results(search_text: str, pageid: str):
    pageid = int(pageid)
    fields = ["title", "aid"]  # 需要搜索的字段
    query = {
        "$or": [
            {field: {"$regex": search_text, "$options": "i"}}
            for field in fields
        ]
    }
    for client in clients["db2"]:
        # try:  # in case one of the db brokedown
            total_article_num = client.info.article.count_documents(query)
            total_page_num = total_article_num // ITEM_PER_PAGE + 1
            res = client.info.article.find(query).skip((pageid - 1) * ITEM_PER_PAGE).limit(ITEM_PER_PAGE)
            article_list = list(res)
            for article in article_list:
                article["cover"] = find_file_path(article["image"].split(",")[0])
                article["date"] = time.strftime("%Y-%m-%d %H:%M", time.localtime(int(article["timestamp"]) / 1000))
                del article["_id"]
            break
        # except:
        #     pass
    return render_template("search_article_results.html", pageid=pageid, article_list=article_list, item_per_page=ITEM_PER_PAGE, total_page_num=total_page_num, last_search_text=search_text, total_article_num=total_article_num)


@app.route("/frontend/article/<aid>")
def get_article(aid: str):
    return render_template("article_info.html", **article_by_id(aid))


@app.route("/frontend/user/<uid>")
def get_user(uid: str):
    user = user_by_id(uid)
    reading_list = find_user_read_list(uid)
    tmp_list = []
    for read in reading_list:
        for client in clients["db2"]:
            # try:
                article = client.info.article.find_one({"aid": read["aid"]})
                break
            # except:
            #     pass
        article["cover"] = find_file_path(article["image"].split(",")[0])
        
        read["date"] = time.strftime("%Y-%m-%d %H:%M", time.localtime(int(read["timestamp"]) / 1000))
        del article["_id"], article["id"], article["aid"], read["_id"], read["id"], read["timestamp"]
        tmp_list.append(dict(**article, **read))
    reading_list = tmp_list
    # print(reading_list)
    for i in range(len(reading_list)):
        reading_list[i]["url"] = f"/frontend/article/{reading_list[i]['aid']}"
    return render_template("user_info.html", user=user, reading_list=reading_list)

@app.route("/frontend/popular_rank/<grainaty>/<date>")
def get_popular_rank(grainaty: str, date:str):
    #transform date string into timestamp and adjust date according to granularity
    if grainaty == "daily":
        date = datetime.strptime(date, "%Y-%m-%d").timestamp()
    elif grainaty == "monthly":
        date = datetime.strptime(date, "%Y-%m-%d").replace(day=1).timestamp()
        #print(date)
    else:
        dt = datetime.strptime(date, "%Y-%m-%d")
        weekstart = dt - timedelta(days=dt.weekday())
        #print(weekstart)
        date = weekstart.timestamp()
    popular = get_popular_by_granularity(grainaty, date)
    popular = dict(popular)
    #print(popular)
    #date not found
    if "message" in popular:
         return render_template("popular_not_found.html", message=popular)
    
    #transform date to fit with website
    if grainaty == "daily":
         popular["date"] = time.strftime("%Y-%m-%d" , time.localtime(int(popular["timestamp"])))
         dt = datetime.strptime(popular["date"], "%Y-%m-%d")
    elif grainaty == "monthly":
         popular["date"] = time.strftime("%B %Y" , time.localtime(int(popular["timestamp"])))
         dt = datetime.strptime(popular["date"], "%B %Y")
    else:
         popular["date"] = "Week " + time.strftime("%W, %Y" , time.localtime(int(popular["timestamp"])))
         dt = popular["date"].split(",")
         week_1 = dt[0].split()
    # get top5 articles and their views that day/week/month
    tmp_list=[]
    for popular_item in popular["articleAidList"]:
        for client in clients["db2"]:
            #print(popular_item["aid"])
            article = client.info.article.find_one({"aid": popular_item["aid"]})
            #views = client.history.be_read.find_one({"aid": popular_item["aid"]})
            if grainaty == "daily":
                views = client.history.be_read.aggregate([
                    {"$match": {"aid": popular_item["aid"]}}, #filter for this aid
                    {"$unwind": "$timestamp"}, #transform timestamp to date
                    {"$addFields": {"timestampDate": {"$toDate": {"$convert": {"to": "double", "input": "$timestamp"}}}}},
                    {"$project": {
                        "year": {"$year": "$timestampDate"},
                        "month": {"$month": "$timestampDate"},
                        "day": {"$dayOfMonth": "$timestampDate"},
                        "aid": "$aid",
                    }},
                    {"$match": {"$and" : [{"year": dt.year},{"month":dt.month}, {"day": dt.day}]}},
                    {"$group": {"_id": {"aid": "$aid"},
                        "readNum": {"$sum": 1},}},
                ])
                views = list(views)
            elif grainaty == "monthly":
                views = client.history.be_read.aggregate([
                    {"$match": {"aid": popular_item["aid"]}}, #filter for this aid
                    {"$unwind": "$timestamp"}, #transform timestamp to date
                    {"$addFields": {"timestampDate": {"$toDate": {"$convert": {"to": "double", "input": "$timestamp"}}}}},
                    {"$project": {
                        "year": {"$year": "$timestampDate"},
                        "month": {"$month": "$timestampDate"},
                        "aid": "$aid",
                    }},
                    {"$match": {"$and" : [{"year": dt.year},{"month":dt.month},]}},
                    {"$group": {"_id": {"aid": "$aid"},
                        "readNum": {"$sum": 1},}},
                ])
                views = list(views)
            else:
                views = client.history.be_read.aggregate([
                    {"$match": {"aid": popular_item["aid"]}}, #filter for this aid
                    {"$unwind": "$timestamp"}, #transform timestamp to date
                    {"$addFields": {"timestampDate": {"$toDate": {"$convert": {"to": "double", "input": "$timestamp"}}}}},
                    {"$project": {
                        "year": {"$isoWeekYear": "$timestampDate"}, # Extract year (ISO-8601 standard)
                        "week": {"$isoWeek": "$timestampDate"},     # Extract week number
                        "aid": "$aid",
                    }},
                    {"$match": {"$and" : [{"year": int(dt[1])},{"week":int(week_1[1])},]}},
                    {"$group": {"_id": {"aid": "$aid"},
                        "readNum": {"$sum": 1},}},
                ])
                views = list(views)
            
            break
        article["views"] = views[0]["readNum"]
        article["cover"] = find_file_path(article["image"].split(",")[0])
        del article["_id"], article["id"], article["aid"], article["timestamp"]
        tmp_list.append(dict(**article, **popular_item))
        #print(tmp_list)
    for i in range(5):
        tmp_list[i]["url"] = f"/frontend/article/{tmp_list[i]['aid']}"
    return render_template("popularRank.html", popular=popular, top5_list=tmp_list)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
