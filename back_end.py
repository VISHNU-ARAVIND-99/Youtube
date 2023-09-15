import googleapiclient.discovery
import pandas as pd
from pymongo import MongoClient
import urllib.parse
import sqlite3


def api_connection():
    youtube_api_key = "Enter your Key"
    api_service_name = "youtube"
    api_version = "v3"
    return googleapiclient.discovery.build(api_service_name, api_version, developerKey=youtube_api_key)


def basic1(q, youtube_object):
    request = youtube_object.search().list(
            part="id,snippet",
            channelType="any",
            maxResults=1,
            q=q,
        )
    return request.execute()


def basic2(p, youtube_object):
    request = youtube_object.channels().list(
        part="snippet,contentDetails,statistics",
        id=p
    )
    return request.execute()


def video_ids(uploads_id, youtube_object):
    request = youtube_object.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=uploads_id,
        maxResults=50
    )
    r = request.execute()
    video_id = []
    for i in r['items']:
        video_id.append(i['contentDetails']['videoId'])

    next_page_token = r.get('nextPageToken')
    while next_page_token is not None:
        request = youtube_object.playlistItems().list(
            part='contentDetails',
            playlistId=uploads_id,
            maxResults=50,
            pageToken=next_page_token)
        response = request.execute()

        for item in response['items']:
            video_id.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
    return video_id


def get_video_details(ids_video, youtube_object):
    video_details = []
    count = 0
    for i in ids_video:
        request = youtube_object.videos().list(
            part="snippet,contentDetails,statistics",
            id=i
        )
        response = request.execute()
        dic = {
            "channelTitle": response["items"][0]["snippet"]["channelTitle"],
            "channelId": response["items"][0]["snippet"]["channelId"],
            "videoId": response["items"][0]["id"],
            "publishedAt": response["items"][0]["snippet"]["publishedAt"],
            "video_title": response["items"][0]["snippet"]["title"],
            "viewCount": response["items"][0]["statistics"]["viewCount"],
            "likeCount": response["items"][0]["statistics"]["likeCount"],
            "commentCount": response["items"][0]["statistics"]["commentCount"],
            "description": response["items"][0]["snippet"]["description"],
        }
        video_details.append(dic)
        count += 1
        if count == 500:
            break
    return video_details


def comment_from_video_id(ids_video, youtube_object):
    comment_details = []
    count = 0
    for i in ids_video:
        request = youtube_object.commentThreads().list(
            part="snippet,replies",
            videoId=i
        )

        response = request.execute()
        for j in response["items"]:
            dic = {
                "commentId": j["id"],
                "videoId": j["snippet"]["videoId"],
                "comment_author": j["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                "comment_text": j["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                "comment_likeCount": j["snippet"]["topLevelComment"]["snippet"]["likeCount"],
                "comment_publishedAt": j["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
            }
            comment_details.append(dic)
        count += 1
        if count == 500:
            break
    return comment_details


def mangodb_upload(collection_name, data):
    for i in data:
        username = urllib.parse.quote_plus("enter your mongodb username")
        password = urllib.parse.quote_plus("enter your mongodb password")

        client = MongoClient(f"mongodb+srv://{username}:{password}"
                             f"@vishnuaravind.se3bvtj.mongodb.net/?retryWrites=true&w=majority")
        db = client["youtube"]
        coll = db[collection_name]
        coll.insert_many(i)
    return f"{collection_name} - Youtube channel Uploded to MondoDb"


def mongodb_to_sql(collection_name):
    username = urllib.parse.quote_plus("enter your mongodb username")
    password = urllib.parse.quote_plus("enter your mongodb password")

    client = MongoClient(f"mongodb+srv://{username}:{password}"
                         f"@vishnuaravind.se3bvtj.mongodb.net/?retryWrites=true&w=majority")
    db = client["youtube"]
    coll = db[collection_name]

    a = [coll.find_one({}, {"_id": 0})]
    df1 = pd.DataFrame(a)
    df1["publishedAt"] = pd.to_datetime(df1["publishedAt"])
    df1['Published_Year'] = df1['publishedAt'].dt.year
    df1['Published_Month'] = df1['publishedAt'].dt.month.map(
        {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov',
         12: 'Dec'})
    df1["subscriberCount"] = df1["subscriberCount"].astype(int)
    df1["viewCount"] = df1["viewCount"].astype(int)
    df1["videoCount"] = df1["videoCount"].astype(int)

    query1 = {"video_title": {"$exists": True}, "$expr": {"$size": {"$objectToArray": "$$ROOT"}}}
    documents1 = coll.find(query1, {"_id": 0})
    i1 = []
    for document in documents1:
        i1.append(document)
    df2 = pd.DataFrame(i1)
    df2["publishedAt"] = pd.to_datetime(df2["publishedAt"])
    df2['Published_Year'] = df2['publishedAt'].dt.year
    df2['Published_Month'] = df2['publishedAt'].dt.month.map(
        {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov',
         12: 'Dec'})
    df2["viewCount"] = df2["viewCount"].astype(int)
    df2["commentCount"] = df2["commentCount"].astype(int)
    df2["likeCount"] = df2["likeCount"].astype(int)

    query2 = {"comment_text": {"$exists": True}, "$expr": {"$size": {"$objectToArray": "$$ROOT"}}}
    documents2 = coll.find(query2, {"_id": 0})
    i2 = []
    for document in documents2:
        i2.append(document)
    df3 = pd.DataFrame(i2)
    df3["comment_publishedAt"] = pd.to_datetime(df3["comment_publishedAt"])
    df3['Published_Year'] = df3['comment_publishedAt'].dt.year
    df3['Published_Month'] = df3['comment_publishedAt'].dt.month.map(
        {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov',
         12: 'Dec'})
    df3["comment_likeCount"] = df3["comment_likeCount"].astype(int)

    conn = sqlite3.connect("youtube.db")
    df1.to_sql('Basic_channel_table', conn, if_exists='append')
    df2.to_sql('video_table', conn, if_exists='append')
    df3.to_sql('comment_table', conn, if_exists='append')
    conn.close()
    return f"{collection_name} youtube channel transformed to Sql from MongoDb"


def list_of_collections():
    username = urllib.parse.quote_plus("vishnuaravind")
    password = urllib.parse.quote_plus("Guvi@123")

    client = MongoClient(f"mongodb+srv://{username}:{password}"
                         f"@vishnuaravind.se3bvtj.mongodb.net/?retryWrites=true&w=majority")
    db = client["youtube"]
    return db.list_collection_names()


def last(question):
    conn = sqlite3.connect("youtube.db")
    if question[0] == "1" and question[1] == "0":
        return pd.read_sql("SELECT video_title, MAX(commentCount), channelTitle FROM video_table", conn)
    elif question[0] == "1":
        return pd.read_sql("SELECT video_title, channelTitle FROM video_table", conn)
    elif question[0] == "2":
        return pd.read_sql("SELECT MAX(videoCount), channelTitle FROM Basic_channel_table", conn)
    elif question[0] == "3":
        return pd.read_sql("SELECT viewCount, video_title, channelTitle FROM video_table ORDER BY viewCount DESC "
                           "LIMIT 10", conn)
    elif question[0] == "4":
        return pd.read_sql("SELECT video_title, commentCount FROM video_table", conn)
    elif question[0] == "5":
        return pd.read_sql("SELECT video_title, MAX(likeCount), channelTitle FROM video_table", conn)
    elif question[0] == "6":
        return pd.read_sql("SELECT viewCount+likeCount, video_title FROM video_table", conn)
    elif question[0] == "7":
        return pd.read_sql("SELECT viewCount, channelTitle FROM Basic_channel_table", conn)
    elif question[0] == "8":
        return pd.read_sql("SELECT Published_Year, video_title FROM video_table WHERE Published_Year = 2022 ", conn)
    else:
        return pd.read_sql("SELECT video_title, description, channelTitle FROM video_table", conn)


def first(name):
    youtube_object = api_connection()

    b1 = basic1(q=name, youtube_object=youtube_object)

    channel_id = b1["items"][0]["snippet"]["channelId"]

    b2 = basic2(p=channel_id, youtube_object=youtube_object)

    uploads_id = b2["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    ids_video = video_ids(uploads_id=uploads_id, youtube_object=youtube_object)

    basic = [{
        "channelTitle": b1["items"][0]["snippet"]["channelTitle"],
        "channelId": b1["items"][0]["snippet"]["channelId"],
        "description": b2["items"][0]["snippet"]["description"],
        "publishedAt": b2["items"][0]["snippet"]["publishedAt"],
        "subscriberCount": b2["items"][0]["statistics"]["subscriberCount"],
        "viewCount": b2["items"][0]["statistics"]["viewCount"],
        "videoCount": b2["items"][0]["statistics"]["videoCount"],
        "uploads_id": b2["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
    }]
    videos = get_video_details(ids_video, youtube_object=youtube_object)
    comment = comment_from_video_id(ids_video, youtube_object=youtube_object)
    data = [basic, videos, comment]
    return [mangodb_upload(collection_name=basic[0]["channelTitle"], data=data), basic]
