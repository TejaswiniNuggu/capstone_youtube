import streamlit as st
import pymongo
from googleapiclient.discovery import build
import mysql.connector
import re
import time
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
from googleapiclient.errors import HttpError
from bson import ObjectId

api_key = "AIzaSyCN_qQFtYR8NA1rfG81uaGgUWddShXWrzc"
# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb+srv://tejaswininuggu:Tejudatabase@cluster0.g7gvc.mongodb.net")
mongo_db = mongo_client["final_capstone_db"]
mongo_collection = mongo_db["final_capstone_coll"]

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "sql1795@",  # Replace with your MySQL password
    "database": "capstone"
}

# Function to establish a MySQL database connection
def connect_to_mysql():
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            return connection
    except mysql.connector.Error as e:
        st.error(f"Error: {e}")
    return None

# Function to parse video duration
def parse_duration(duration_str):
    match = re.match(r'PT(\d+)M(\d+)S', duration_str)
    if match:
        minutes = int(match.group(1))
        seconds = int(match.group(2))
        return minutes * 60 + seconds
    else:
        return 0

# Function to insert data into MongoDB
def insert_data_to_mongo(data):
    mongo_collection.insert_many([data])

# Function to insert channel data into both MongoDB and MySQL
def insert_channel_data(channel_id):
    try:
        # Connect to YouTube API and fetch channel details
        #api_key = "AIzaSyDQc6P_5AXuV53j3BtHyOQ1x-3gXIU9Si8"  # Replace with your YouTube API key
        youtube = build("youtube", "v3", developerKey=api_key)

        request = youtube.channels().list(part="snippet,statistics", id=channel_id)
        response = request.execute()
        channel_details = response["items"][0]["snippet"]
        channel_stats = response["items"][0]["statistics"]

        # Extract channel data
        data_to_insert = {
            "channel_id": channel_id,
            "channel_name": channel_details["title"],
            "subscriber_count": channel_stats["subscriberCount"],
            "channel_type": "kind",  # Replace with the actual channel type if available
            "channel_description": channel_details["description"],
            "channel_views": channel_stats["viewCount"]
        }

        # Insert channel data into MongoDB
        insert_data_to_mongo(data_to_insert)

        # Insert channel data into MySQL
        db_connection = connect_to_mysql()
        cursor = db_connection.cursor()
        cursor.execute(
            'INSERT IGNORE INTO channels_data_f (channel_id, channel_name, subscriber_count, channel_type, channel_description, channel_views) VALUES (%s, %s, %s, %s, %s, %s)',
            (data_to_insert["channel_id"], data_to_insert["channel_name"], data_to_insert["subscriber_count"],
             data_to_insert["channel_type"], data_to_insert["channel_description"], data_to_insert["channel_views"])
        )
        db_connection.commit()

        st.success(f"Channel data for channel ID {channel_id} migrated successfully.")

    except Exception as e:
        st.error(f"Error retrieving channel data for channel ID {channel_id}: {e}")

# Function to get playlists for a channel and store them in MongoDB and MySQL
def get_playlists_for_channel(channel_id):
    try:
        # Connect to YouTube API and fetch playlist details for the channel
        #api_key = "AIzaSyDQc6P_5AXuV53j3BtHyOQ1x-3gXIU9Si8"  # Replace with your YouTube API key
        youtube = build("youtube", "v3", developerKey=api_key)

        request = youtube.playlists().list(part="snippet", channelId=channel_id, maxResults=10)
        response = request.execute()
        playlist_details = []

        for item in response.get("items", []):
            playlist_info = {
                "playlist_id": item["id"],
                "playlist_title": item["snippet"]["title"],
                "channel_id": channel_id
            }
            playlist_details.append(playlist_info)

        # Insert playlist data into MongoDB
        for playlist in playlist_details:
            mongo_collection.insert_many([playlist])

        # Insert playlist data into MySQL
        db_connection = connect_to_mysql()
        cursor = db_connection.cursor()
        for playlist in playlist_details:
            cursor.execute(
                'INSERT IGNORE INTO playlists (playlist_id, playlist_title, channel_id) VALUES (%s, %s, %s)',
                (playlist["playlist_id"], playlist["playlist_title"], playlist["channel_id"])
            )

        db_connection.commit()
        st.success(f"Playlist data for channel ID {channel_id} migrated successfully.")

    except Exception as e:
        st.error(f"Error retrieving playlist data for channel ID {channel_id}: {e}")

# Function to retrieve video details for a channel and store them in MongoDB and MySQL
def get_videos_for_channel(channel_id):
    try:
        # Connect to YouTube API and fetch video details for the channel
        #api_key = "AIzaSyDQc6P_5AXuV53j3BtHyOQ1x-3gXIU9Si8"  # Replace with your YouTube API key
        youtube = build("youtube", "v3", developerKey=api_key)

        request = youtube.search().list(part="id", channelId=channel_id, maxResults=50)  # Increase maxResults as needed
        response = request.execute()
        video_ids = []

        for item in response.get("items", []):
            if item.get("id", {}).get("kind") == "youtube#video":
                video_ids.append(item["id"]["videoId"])

        video_details = []

        for video_id in video_ids:
            request = youtube.videos().list(part="contentDetails,snippet,statistics", id=video_id)
            response = request.execute()
            video_content = response["items"][0]["contentDetails"]
            video_snippet = response["items"][0]["snippet"]
            video_statistics = response["items"][0]["statistics"]

            duration_str = video_content["duration"]
            duration_seconds = parse_duration(duration_str)

            video_info = {
                "video_id": video_id,
                "title": video_snippet["title"],
                "published_at": datetime.strptime(video_snippet["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"),
                "description": video_snippet["description"],
                "view_count": int(video_statistics["viewCount"]),
                "like_count": int(video_statistics.get("likeCount", 0)),
                "fav_count": int(video_statistics.get("favoriteCount", 0)),
                "comment_count": int(video_statistics.get("commentCount", 0)),
                "duration": duration_seconds,
                "thumbnail": video_snippet["thumbnails"],
                "channel_id": channel_id,
            }

            video_details.append(video_info)

            # Insert video data into MySQL
            db_connection = connect_to_mysql()
            cursor = db_connection.cursor()
            cursor.execute(
                'INSERT IGNORE INTO videos (video_id, title, published_at, description, view_count, like_count, fav_count, comment_count, duration, thumbnail,channel_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (
                    video_info["video_id"], video_info["title"], video_info["published_at"],
                    video_info["description"],
                    video_info["view_count"], video_info["like_count"], video_info["fav_count"],
                    video_info["comment_count"], video_info["duration"], str(video_info["thumbnail"]),
                    video_info["channel_id"]
                )
            )
            db_connection.commit()

        return video_details

    except Exception as e:
        st.error(f"Error retrieving video data for channel ID {channel_id}: {e}")
        return []
# Function to retrieve comments for a specific video and insert them into the comments table
def get_comments_for_video(video_id):
    #api_key = "AIzaSyDQc6P_5AXuV53j3BtHyOQ1x-3gXIU9Si8"  # Replace with your YouTube API key
    youtube = build("youtube", "v3", developerKey=api_key)

    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=10  # Adjust this as needed
        )
        response = request.execute()
        comments = []

        for item in response.get("items", []):
            comment_info = {
                "comment_id": item["id"],
                "author": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                "comment_text": item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                "published_at": datetime.strptime(item["snippet"]["topLevelComment"]["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"),
                "video_id": video_id
            }
            comments.append(comment_info)

        return comments
    except HttpError as e:
        if e.resp.status == 403:
            # Comments are disabled for this video, handle accordingly
            return []
        else:
            raise e
# Function to retrieve video IDs for a specific channel
def get_video_ids_for_channel(channel_id):
    try:
        # Connect to YouTube API and fetch video IDs for the channel
        #api_key = "AIzaSyDQc6P_5AXuV53j3BtHyOQ1x-3gXIU9Si8"  # Replace with your YouTube API key
        youtube = build("youtube", "v3", developerKey=api_key)

        video_ids = []
        next_page_token = None

        while True:
            request = youtube.search().list(
                part="id",
                channelId=channel_id,
                maxResults=10,  # You can adjust this batch size as needed
                pageToken=next_page_token
            )
            response = request.execute()

            for item in response.get("items", []):
                if item["id"]["kind"] == "youtube#video":
                    video_ids.append(item["id"]["videoId"])

            next_page_token = response.get("nextPageToken")

            if not next_page_token:
                break

        return video_ids
    except Exception as e:
        st.error(f"Error retrieving video IDs for channel ID {channel_id}: {e}")
        return []


# Function to retrieve and insert comments for all videos associated with a channel
def retrieve_and_insert_comments(channel_id):
    try:
        # Retrieve video IDs associated with the channel
        video_ids = get_video_ids_for_channel(channel_id)

        for video_id in video_ids:
            comments = get_comments_for_video(video_id)
            if comments:
                # Generate and assign BSON ObjectIDs for comments
                for comment in comments:
                    comment["_id"] = ObjectId()

                # Insert comments into MongoDB
                mongo_collection.insert_many(comments)

                # Insert comments into the SQL table
                db_connection = connect_to_mysql()
                cursor = db_connection.cursor()
                for comment in comments:
                    cursor.execute(
                        'INSERT INTO comments (comment_id, author, comment_text, published_at, video_id) VALUES (%s, %s, %s, %s, %s)',
                        (str(comment['_id']), comment['author'], comment['comment_text'], comment['published_at'], comment['video_id'])
                    )
                db_connection.commit()

                st.success(f"Comments for video ID {video_id} migrated successfully.")

    except Exception as e:
        st.error(f"Error retrieving or inserting comment data for channel ID {channel_id}: {e}")

# Streamlit UI
option=["want to insert channel id","display sql query to retrive information"]
selected_opt = st.selectbox("Select a option", option)
if selected_opt == "want to insert channel id":
    st.title("YouTube Data Migration App")

    # Input for channel IDs
    channel_ids = st.text_area("Enter 10 YouTube Channel IDs (one per line)", height=100).strip().split('\n')

    if len(channel_ids) != 10:
        st.warning("Please enter exactly 10 YouTube Channel IDs.")
    else:
        # Create a dropdown for selecting a channel ID
        channel_id_in = st.selectbox("Select a YouTube Channel ID", channel_ids)
        if st.button(f"Retrieve Channel Details for {channel_id_in}"):
            # Insert channel data into MongoDB and MySQL
            insert_channel_data(channel_id_in)

        if st.button(f"Retrieve Playlist Details for {channel_id_in}"):
            # Insert playlist data into MongoDB and MySQL
            get_playlists_for_channel(channel_id_in)

        if st.button(f"Retrieve Video Details for {channel_id_in}"):
            # Retrieve video details and insert into MongoDB and MySQL
            video_details = get_videos_for_channel(channel_id_in)
            st.success(f"Video data for channel ID {channel_id_in} migrated successfully.")

        if st.button(f"Retrieve Comment Details for {channel_id_in}"):
            # Retrieve and insert comments for videos associated with the channel
            retrieve_and_insert_comments(channel_id_in)


elif selected_opt == "display sql query to retrive information":

    # Streamlit UI for displaying data from the SQL Data Warehouse
    # Streamlit UI for displaying data from the SQL Data Warehouse
    if st.checkbox("Display Data from SQL Data Warehouse"):
        st.subheader("Data from SQL Data Warehouse")

        # Establish MySQL database connection
        db_connection = connect_to_mysql()

        if db_connection:
            st.success("Connected to MySQL database")
            cursor = db_connection.cursor()
            ques=["What are the names of all the videos and their corresponding channels?",
                  "Which channels have the most number of videos, and how many videos do they have?",
                  "What are the top 10 most viewed videos and their respective channels?",
                   "How many comments were made on each video, and what are their corresponding video names?",
                   "Which videos have the highest number of likes, and what are their  corresponding channel names?",
                   "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                  "What is the total number of views for each channel, and what are their corresponding channel names?",
                   "What are the names of all the channels that have published videos in the year 2022?",
                  "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                   "Which videos have the highest number of comments, and what are their corresponding channel names?"]
            # Execute SQL query to fetch data (modify the query as needed)
            selected_q = st.selectbox("Select a ques", ques)
            if selected_q=='What are the names of all the videos and their corresponding channels?':
                cursor.execute(" SELECT videos.title AS video_title, channels_data_f.channel_name AS channel_name FROM videos INNER JOIN channels_data_f ON videos.channel_id = channels_data_f.channel_id;")  # Replace with your table name
                data_from_sql = cursor.fetchall()
            elif selected_q=='Which channels have the most number of videos, and how many videos do they have?':
                cursor.execute("select channels_data_f.channel_id, channels_data_f.channel_name, count(video_id) as video_count from channels_data_f left join videos on channels_data_f.channel_id=videos.channel_id group by channels_data_f.channel_id, channels_data_f.channel_name order by 3 desc;")
                data_from_sql = cursor.fetchall()
            elif selected_q == 'What are the top 10 most viewed videos and their respective channels?':
                cursor.execute("select video_id, title, view_count, channel_name from videos left join channels_data_f on videos.channel_id = channels_data_f.channel_id order by view_count desc limit 10;")
                data_from_sql = cursor.fetchall()
            elif selected_q == 'How many comments were made on each video, and what are their corresponding video names?':
                cursor.execute("select video_id,title, comment_count from videos order by 3 desc;")
                data_from_sql = cursor.fetchall()
            elif selected_q == 'Which videos have the highest number of likes, and what are their corresponding channel names?':
                cursor.execute("select video_id, title,like_count,channel_name from videos left join channels_data_f on "
                               "videos.channel_id=channels_data_f.channel_id order by like_count limit 1;")
                data_from_sql = cursor.fetchall()
            elif selected_q == 'What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
                cursor.execute('select title,like_count as likes from videos;')
                data_from_sql = cursor.fetchall()
            elif selected_q == 'What is the total number of views for each channel, and what are their corresponding channel names?':
                cursor.execute("select channel_id, channel_name, channel_views from channels_data_f order by 3 desc;")
                data_from_sql = cursor.fetchall()
            elif selected_q == 'What are the names of all the channels that have published videos in the year 2022?':
                cursor.execute(("select distinct channel_name from videos left join channels_data_f on videos.channel_id=channels_data_f.channel_id where year(published_at) = 2022;"))
                data_from_sql = cursor.fetchall()
            elif selected_q == 'What is the average duration of all videos in each channel, and what are their corresponding channel names?':
                cursor.execute("select videos.channel_id,channel_name,avg(duration) as duration from videos left join channels_data_f on videos.channel_id=channels_data_f.channel_id group by videos.channel_id,channel_name;")
                data_from_sql = cursor.fetchall()
            elif selected_q == 'Which videos have the highest number of comments, and what are their corresponding channel names?':
                cursor.execute("select video_id,title,comment_count,channel_name from videos left join channels_data_f on videos.channel_id=channels_data_f.channel_id order by comment_count desc limit 1;")
                data_from_sql = cursor.fetchall()
            else:
                st.write("invalid input")
            # Display data in a table or any other format you prefer
            if data_from_sql:
                st.write("Data from SQL Data Warehouse:")
                for row in data_from_sql:
                    st.write(row)

            # Close the MySQL connection
            db_connection.close()
            st.success("MySQL connection closed")
