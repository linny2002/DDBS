from flask import Flask, render_template
import requests
from pymongo import MongoClient


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
ITEM_PER_PAGE = 100


# text, image and video files of an article is stored in the dfs
def find_file_path(file_name):
    for client in sum(list(clients.values()), []):
        try:
            res = client.mapping.article.find_one({"name": file_name})
            return res["path"]
        except:
            pass
    return None


def user_by_id(uid):
    for client in sum(list(clients.values()),[]):
        try:
            user = client.info.user.find_one({"uid": uid})
            if user:
                return user
        except:
            pass
    return {"message": f"User {uid} not found."}


def article_by_id(aid):
    for client in clients["db2"]:
        try:
            article = client.info.article.find_one({"aid": aid})
            text_file = article["text"]
            text_path = find_file_path(text_file).replace("0.0.0.0", "nginx").strip()
            text = requests.get(text_path).text
            images = [find_file_path(i).replace("0.0.0.0", "localhost").strip() for i in article["image"].split(",") if i.strip()]
            videos = [find_file_path(i).replace("0.0.0.0", "localhost").strip() for i in article["video"].split(",") if i.strip()] # in the form of url in flv
            return dict(text=text, images=images, videos=videos)
        except:
            pass
        

def find_user_read_list(uid):
    for client in sum(list(clients.values()),[]):
        try:
            history = list(client.history.read.find(dict(uid=uid)))
            if history:
                return history
        except:
            pass
    return []


@app.route("/frontend/user_list/<pageid>")
def user_list_page(pageid: int):
    if not pageid:
        pageid = 1
    for client in clients["db1"]:
        try:
            db1_user_count = len(list(client.info.user.find()))
        except:
            pass
    if pageid * ITEM_PER_PAGE < db1_user_count:
        target_clients = clients["db1"]
    else:
        target_clients = clients["db2"]
    for client in target_clients:
        try:
            res = client.info.user.find().skip((pageid - 1) * ITEM_PER_PAGE - 1).limit(ITEM_PER_PAGE)
            user_list = list(res)
        except:
            pass
    return render_template("user_list.html", user_list=user_list)


@app.route("/frontend/article_list/<pageid>")
def article_list_page(pageid: int):
    if not pageid:
        pageid = 1
    for client in clients["db2"]:
        try:  # in case one of the db brokedown
            res = client.info.article.find().skip((pageid - 1) * ITEM_PER_PAGE - 1).limit(ITEM_PER_PAGE)
            article_list = list(res)
        except:
            pass
    return render_template("article_list.html", article_list=article_list)


@app.route("/frontend/search/<search_text>")
def get_search_results(search_text: str):
    
    return render_template("searched_results.html")


@app.route("/frontend/article/<aid>")
def get_article(aid: str):
    return render_template("article_info.html", aid=aid, **article_by_id(aid))


@app.route("/frontend/user/<uid>")
def get_user(uid: str):
    user = user_by_id(uid)
    reading_list = find_user_read_list(uid)
    reading_list = [dict(text=article_by_id(i["aid"])["text"], **i) for i in reading_list]
    for i in range(len(reading_list)):
        reading_list[i]["location"] = f"/frontend/article/{reading_list[i]['aid']}"
        del reading_list[i]["_id"]
    return render_template("user_info.html", user=user, reading_list=reading_list)


@app.route("/frontend/popular_rank/<grainaty>/<rid>")
def get_popular_rank_route(grainaty: str):
    pass


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
