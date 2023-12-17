from urllib.parse import quote
from sqlalchemy import create_engine
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy.exc import InterfaceError
from googleapiclient.discovery import build
import pandas as pd
import pymongo
import seaborn as sns
import pymysql
from datetime import datetime
import streamlit as st



# API key connection:
def connect():
    api_id = "AIzaSyDICyj4LVellsRz16kVqaVvMT8XFL6kM8c"
    
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube=build(api_service_name,api_version,developerKey=api_id)
   
    return youtube

youtube=connect()


# get channel information:

def Channel_Info(channel_id):            # pass argument channel id:
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id                 
    )
    response = request.execute()
    
    for i in response["items"]:
        data=dict(Channel_Name=i["snippet"]["title"],
              Channel_Id=i["id"],
              Subscribers=int(i["statistics"]["subscriberCount"]),
              Views=int(i["statistics"]["viewCount"]),
              Total_Videos=int(i["statistics"]["videoCount"]),
              Channel_Description=i["snippet"]["description"],
              Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"]
             )
    return data


# CHANNEL IDS:
# software & web : UCRnpZX5_L3qPGTgnBBF6B2w
# logic first : UCXhbCCZAG4GlaBLm80ZL-iA
# arivu channel :UCBcy0sGa8AsyJH9CoaDPNhg


# get video ids:

def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part="contentDetails").execute()
    playlist_id=response["items"][0]["contentDetails"]['relatedPlaylists'][ 'uploads']
    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part="snippet",
                                            playlistId=playlist_id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1["items"])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token=response1.get("nextPageToken")

        if next_page_token is None:
            break
    return video_ids

import re
def convert_duration(duration):
            regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
            match = re.match(regex, duration)
            if not match:
                return '00:00:00'
            hours, minutes, seconds = match.groups()
            hours = int(hours[:-1]) if hours else 0
            minutes = int(minutes[:-1]) if minutes else 0
            seconds = int(seconds[:-1]) if seconds else 0
            total_seconds = hours * 3600 + minutes * 60 + seconds
            return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60),
                                                 int(total_seconds % 60))


# get video information:
                                              
def video_info(video_ids):
    video_data=[]
    for i in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=i
        )
        response=request.execute()

        items = response.get('items', [])
        if items:
            video = items[0]

            duration = video.get('contentDetails', {}).get('duration', 'Not Available')
            if duration != 'Not Available':
                duration = convert_duration(duration)

            # Use `snippet` variable within the loop
            snippet = video.get('snippet', {})
            statistics = video.get('statistics', {})

        for i in response["items"]:
            data={'Channel_Name': snippet.get("channelTitle"),
                'Channel_id': snippet.get("channelId"),
                'Video_id': video.get("id"),
                'title': snippet.get('title'),
                'Tags': snippet.get('tags'),
                'Thumbnails': snippet.get("thumbnails", {}).get('default', {}).get('url'),
                'description': snippet.get('description'),
                'duration': duration,
                'published_at': snippet.get('publishedAt'),
                'views': int(statistics.get("viewCount", 0)),
                'Likes': int(statistics.get("likeCount", 0)),
                'comments': int(statistics.get("commentCount", 0)),
                'favorite': int(statistics.get('favoriteCount', 0)),
                'definition': video.get("contentDetails", {}).get('definition'),
                'caption': video.get("contentDetails", {}).get('caption')
            }
            video_data.append(data)
    return video_data


# get comment information:

def comment_info(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId= video_id,
                maxResults=50

            )
            response=request.execute()

            for item in response["items"]:
                data=dict(comment_id=item["snippet"]["topLevelComment"]["id"],
                         video_id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                         comment_text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                         comment_author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                         comment_published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                comment_data.append(data)
    except:
        pass
    return comment_data


# get playlist details:

def get_playlist_details(channel_id):
    next_page_token=None
    all_data=[]

    while True:
        request=youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(playlist_id=item["id"],
                      title=item["snippet"]["title"],
                      channel_id=item["snippet"]["channelId"],
                      channel_name=item["snippet"]["channelTitle"],
                      published=item["snippet"]["publishedAt"],
                      video_count=item["contentDetails"]["itemCount"]) 
            all_data.append(data)

        next_page_token=response.get("nextPageToken")
        if next_page_token is None:
            break
    return all_data
        


# mongodb connect with python:

client=pymongo.MongoClient("mongodb://localhost:27017")  
db=client["youtube_data"]


def channel_details(channel_id):
     ch_details=Channel_Info(channel_id)
     pl_details=get_playlist_details(channel_id)
     vi_ids=get_videos_ids(channel_id)
     vi_details=video_info(vi_ids)
     com_details=comment_info(vi_ids)
        
     collection=db["channel_details1"]
     collection.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                            "video_information":vi_details,"comment_information": com_details})
     
     return "upload completed successfully"


# connection sql with python:

mydb=pymysql.connect(host="127.0.0.1",
                     user="root",
                     passwd="mysql@123")
cursor=mydb.cursor()


# create database in sql:
try:
    cursor.execute("create database youtube")
    mydb=pymysql.connect(host="127.0.0.1",
                         user="root",
                         passwd="mysql@123",
                         database="youtube")
    cursor=mydb.cursor()
except:
    print("database already created")

     

def channel_table():
    mydb=pymysql.connect(host="127.0.0.1",
                                user="root",
                                passwd="mysql@123",
                                database="youtube")
    cursor=mydb.cursor()
    drop_query='''drop table if exists channels'''        # Avoid duplicate occuring by using drop query:
    cursor.execute(drop_query)
    mydb.commit()

    try:

        cursor.execute('''create table channels(Channel_Name varchar(100),
                                                Channel_Id varchar(80) primary key,
                                                Subscribers int,
                                                Views int,
                                                Total_Videos int,
                                                Channel_Description text,
                                                Playlist_Id varchar(80))''')    
        mydb.commit()

    except:
        print("channel table already created")
        
        # Database connection details
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "mysql@123",
        "database": "youtube",
    }

    # URL encode the password
    encoded_password = quote(db_config["password"])

    # Create a connection string
    connection_url = f'mysql+mysqlconnector://{db_config["user"]}:{encoded_password}@{db_config["host"]}/{db_config["database"]}'

    # Create SQLAlchemy engine
    engine = create_engine(connection_url)
    print("S")
    table_name = 'channels'
    ch_list=[]
    db=client["youtube_data"]
    collection=db["channel_details1"]
    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])

    df=pd.DataFrame(ch_list)
    # Store the DataFrame in the SQL database
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print("OK")



def playlist_table():
    mydb=pymysql.connect(host="127.0.0.1",
                                user="root",
                                passwd="mysql@123",
                                database="youtube")
    cursor=mydb.cursor()
    drop_query='''drop table if exists playlist'''        # Avoid duplicate occuring by using drop query:
    cursor.execute(drop_query)
    mydb.commit()

    try:

        
        cursor.execute('''create table playlist(playlist_id varchar(100) primary key,
                                                title varchar(100),
                                                channel_id varchar(100),
                                                channel_name varchar(100),
                                                published varchar(100),
                                                video_count int)''')


        mydb.commit()


    except:
        print("playlist table already created")
    
        # Database connection details
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "mysql@123",
        "database": "youtube",
    }

    # URL encode the password
    encoded_password = quote(db_config["password"])

    # Create a connection string
    connection_url = f'mysql+mysqlconnector://{db_config["user"]}:{encoded_password}@{db_config["host"]}/{db_config["database"]}'

    # Create SQLAlchemy engine
    engine = create_engine(connection_url)
    print("S")
    table_name = 'playlist'
    pl_list=[]
    db=client["youtube_data"]
    collection=db["channel_details1"]
    for pl_data in collection.find({},{"_id":0,"playlist_information":1}):
        for i in range((len(pl_data["playlist_information"]))):
         pl_list.append(pl_data["playlist_information"][i])

    df1=pd.DataFrame(pl_list)

    # Store the DataFrame in the SQL database
    df1.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print("OK")


# create video table:

def videos_table():
    mydb=pymysql.connect(host="127.0.0.1",
                             user="root",
                             passwd="mysql@123",
                             database="youtube")
    cursor=mydb.cursor()
    drop_query = "drop table if exists video"
    cursor.execute(drop_query)
    mydb.commit()

    cursor.execute('''create table video(Channel_Name varchar(100),
                    Channel_Id varchar(100),
                    Video_Id varchar(30) primary key, 
                    title varchar(150), 
                    Tags text,
                    Thumbnails varchar(200),
                    description text, 
                    duration time, 
                    published_at timestamp,
                    views bigint, 
                    Likes bigint,
                    comments int,
                    favorite int, 
                    definition varchar(10), 
                    caption varchar(50))''')
    mydb.commit()

        # Database connection details
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "mysql@123",
        "database": "youtube",
    }

    # URL encode the password
    encoded_password = quote(db_config["password"])

    # Create a connection string
    connection_url = f'mysql+mysqlconnector://{db_config["user"]}:{encoded_password}@{db_config["host"]}/{db_config["database"]}'

    # Create SQLAlchemy engine
    engine = create_engine(connection_url)
    print("S")
    table_name = 'video'
    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details1"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)

    df2['Likes'] = df2['Likes'].fillna(0)  

    for col in df2.columns:
        if isinstance(df2[col][0], list):
            # If the column contains lists, convert them to strings or flatten them
            df2[col] = df2[col].apply(lambda x: ', '.join(map(str, x)) if x else '')
    # Store the DataFrame in the SQL database
    df2.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print("OK")


def comments_table():
    mydb=pymysql.connect(host="127.0.0.1",
                             user="root",
                             passwd="mysql@123",
                             database="youtube")
    cursor=mydb.cursor()
    drop_query='''drop table if exists comments'''        # Avoid duplicate occuring by using drop query:
    cursor.execute(drop_query)
    mydb.commit()

    # create comment table:


    cursor.execute('''create table comments(comment_id varchar(100) primary key,
                                             video_id varchar(50),
                                             comment_text text,
                                             comment_author varchar(150),
                                             comment_published varchar(100))''')


    mydb.commit()

    
    # Database connection details
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "mysql@123",
        "database": "youtube",
    }

    # URL encode the password
    encoded_password = quote(db_config["password"])

    # Create a connection string
    connection_url = f'mysql+mysqlconnector://{db_config["user"]}:{encoded_password}@{db_config["host"]}/{db_config["database"]}'

    # Create SQLAlchemy engine
    engine = create_engine(connection_url)
    print("S")
    table_name = 'comments'
    com_list=[]
    db=client["youtube_data"]
    collection=db["channel_details1"]
    for com_data in collection.find({},{"_id":0,"comment_information":1}):
        for i in range((len(com_data["comment_information"]))):
            com_list.append(com_data["comment_information"][i])

    df4=pd.DataFrame(com_list)

    # Store the DataFrame in the SQL database
    df4.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print("OK")


def tables():
    channel_table()
    playlist_table()
    comments_table()
    videos_table()
    return "Table created successfully"


def show_ch_table():
    ch_list=[]
    db=client["youtube_data"]
    collection=db["channel_details1"]
    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])

    df=st.dataframe(ch_list)   
    
    return df


def show_pl_table():
    pl_list=[]
    db=client["youtube_data"]
    collection=db["channel_details1"]
    for pl_data in collection.find({},{"_id":0,"playlist_information":1}):
        for i in range((len(pl_data["playlist_information"]))):
            pl_list.append(pl_data["playlist_information"][i])

    df1=st.dataframe(pl_list)
    
    return df1


def show_vi_table():
    vi_list=[]
    db=client["youtube_data"]
    collection=db["channel_details1"]
    for vi_data in collection.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2=st.dataframe(vi_list) 
    
    return df2


def show_com_table():
    com_list=[]
    db=client["youtube_data"]
    collection=db["channel_details1"]
    for com_data in collection.find({},{"_id":0,"comment_information":1}):
        for i in range((len(com_data["comment_information"]))):
            com_list.append(com_data["comment_information"][i])

    df3=st.dataframe(com_list)
    
    return df3


                   # CREATE STREAMLIT:
    
with st.sidebar:
    st.title(":blue[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
    st.header("Data Extraction")
    st.caption("Python")
    st.caption("API Integration")
    st.caption("Youtube Data Collection")
    st.caption("MongoDB")
    st.caption("Data Management using MongoDB and SQL")

# get input from user:

channel_id=st.text_input("Enter the channel_id:")

# create button & after click the button transfer information to mongodb:

if st.button("collect and store data"):
    ch_id=[]
    db=client["youtube_data"]
    collection=db["channel_details1"]
    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_id.append(ch_data["channel_information"]["Channel_Id"])
    
    if channel_id in ch_id:
        st.success("given channel id already exists")
        
    else:
        insert=channel_details(channel_id)
        st.success(insert)

# create table in sql:

if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)
    
show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_ch_table()
    
elif show_table=="PLAYLISTS":
    show_pl_table()
    
elif show_table=="VIDEOS":
    show_vi_table()

elif show_table=="COMMENTS":
    show_com_table()
    


# sql connection:

mydb=pymysql.connect(host="127.0.0.1",
                         user="root",
                         passwd="mysql@123",
                         database="youtube")
cursor=mydb.cursor()

question=st.selectbox("Select Your Question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. Top 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. videos with highest likes",
                                              "6. total number of likes for each videos",
                                              "7. total number of views for each channel",
                                              "8. Videos published in the year of 2022",
                                              "9. Average duration of all videos in each channel",
                                              "10. Videos with highest number of comments"))

if question=="1. All the videos and the channel name":
    query='''select title as videos,channel_name as channelname from video'''
    cursor.execute(query)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)
    
elif question=="2. channels with most number of videos":
    query2='''select Channel_Name as channelname, Total_Videos as no_of_videos from channels order by Total_Videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","no of videos"])
    st.write(df2)


elif question=="3. Top 10 most viewed videos":
    query3='''select views as views,Channel_Name as channelname,Title as videotitle from video where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. comments in each videos":
    query4='''select comments as no_comments,Title as videotitle from video where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no_comments","videotitle"])
    st.write(df4)
    
elif question=="5. videos with highest likes":
    query5='''select Title as videotitle,Channel_Name as channelname,Likes as likecount from video where Likes is not null order by Likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)
    
elif question=="6. total number of likes for each videos":
    query6='''select Likes as likecount,Title as videotitle from video'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)
    

elif question=="7. total number of views for each channel":
    query7='''select Channel_Name as channelname,views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channelname","totalviews"])
    st.write(df7)
    
elif question=="8. Videos published in the year of 2022":
    query8='''select Title as video_title,published_at as videorelease,Channel_Name as channelname from video where extract(year from Published_at)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","Published","Channel_Name"])
    st.write(df8)
    

elif question=="9. Average duration of all videos in each channel":
    query9='''select Channel_Name as channelname,SEC_TO_TIME(AVG(TIME_TO_SEC(duration))) as avgduration from video group by Channel_Name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["Channel_Name","AVG(Duration)"])
    
    T9=[]
    for index,row in df9.iterrows():
        ch_title=row["Channel_Name"]
        avg_duration=row["AVG(Duration)"]
        avg_duration_str=str( avg_duration)
        T9.append(dict(channeltitle=ch_title,avgduration=avg_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)
    
elif question=="10. Videos with highest number of comments":
    query10='''select Title as video_title,Channel_Name as channelname,comments as comments from video where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","Channel_Name","comments"])
    st.write(df10)

    
