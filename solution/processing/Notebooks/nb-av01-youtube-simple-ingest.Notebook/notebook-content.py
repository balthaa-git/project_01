# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# 1. Imports and constants

# CELL ********************

import requests
import json
from datetime import datetime

CHANNEL_ID = "UCrvoIYkzS-RvCEb0x7wfmwQ"

KEY_VAULT_URL = "https://fabric-int-kv.vault.azure.net/"
SECRET_NAME = "youtube-api-key"

BASE_URL = "https://www.googleapis.com/youtube/v3"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

API_KEY = notebookutils.credentials.getSecret(KEY_VAULT_URL, SECRET_NAME)

print("Secret retrieved successfully")
print("API key length:", len(API_KEY))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 2. Function to get channel data

# CELL ********************

def get_channel_data(channel_id, api_key):
    url = f"{BASE_URL}/channels"
    
    params = {
        "part": "snippet,statistics,contentDetails",
        "id": channel_id,
        "key": api_key
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    items = data.get("items", [])
    
    if not items:
        raise ValueError("No channel data returned")
    
    return data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 3. Function to get uploads playlist ID

# CELL ********************

def get_uploads_playlist_id(channel_data):
    items = channel_data.get("items", [])
    channel = items[0]
    
    return channel.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 4. Function to get all playlist items with pagination

# CELL ********************

def get_all_playlist_items(uploads_playlist_id, api_key):
    url = f"{BASE_URL}/playlistItems"
    
    all_items = []
    page_token = None
    page_num = 1
    
    while True:
        params = {
            "part": "snippet,contentDetails",
            "playlistId": uploads_playlist_id,
            "maxResults": 50,
            "key": api_key
        }
        
        if page_token:
            params["pageToken"] = page_token
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        items = data.get("items", [])
        all_items.extend(items)
        
        print(f"Page {page_num}: fetched {len(items)} items")
        
        page_token = data.get("nextPageToken")
        if not page_token:
            break
        
        page_num += 1
    
    return all_items

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_video_ids(playlist_items):
    video_ids = []
    
    for item in playlist_items:
        video_id = item.get("contentDetails", {}).get("videoId")
        if video_id:
            video_ids.append(video_id)
    
    return video_ids

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_video_stats(video_ids, api_key, batch_size=40):
    url = f"{BASE_URL}/videos"
    
    all_video_stats = []
    
    for i in range(0, len(video_ids), batch_size):
        batch_ids = video_ids[i:i + batch_size]
        
        params = {
            "part": "snippet,statistics",
            "id": ",".join(batch_ids),
            "key": api_key
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        batch_items = data.get("items", [])
        all_video_stats.extend(batch_items)
        
        print(f"Fetched batch {(i // batch_size) + 1}: {len(batch_items)} videos")
    
    return all_video_stats

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1: Get channel data
channel_data = get_channel_data(CHANNEL_ID, API_KEY)

# Step 2: Get uploads playlist ID
uploads_playlist_id = get_uploads_playlist_id(channel_data)
print("Uploads playlist ID:", uploads_playlist_id)

# Step 3: Get all playlist items
playlist_items = get_all_playlist_items(uploads_playlist_id, API_KEY)
print("Total playlist items fetched:", len(playlist_items))

# Step 4: Extract video IDs
video_ids = extract_video_ids(playlist_items)
print("Total video IDs extracted:", len(video_ids))
print("First 5 video IDs:", video_ids[:5])

# Step 5: Get video stats
video_stats = get_video_stats(video_ids, API_KEY)
print("Total video stats fetched:", len(video_stats))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

print("Lakehouse workspace name:", variables.LH_WORKSPACE_NAME)
print("Bronze lakehouse name:", variables.BRONZE_LH_NAME)


LH_WORKSPACE_NAME = variables.LH_WORKSPACE_NAME
BRONZE_LH_NAME = variables.BRONZE_LH_NAME

RAW_BASE_PATH = f"abfss://{LH_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{BRONZE_LH_NAME}.Lakehouse/Files/raw/youtube"

print("RAW_BASE_PATH =", RAW_BASE_PATH)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_json_to_onelake(data, folder_path, file_prefix):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = f"{folder_path}/{file_prefix}_{timestamp}.json"
    
    json_text = json.dumps(data, indent=2)
    notebookutils.fs.put(file_path, json_text, True)
    
    print(f"Written file: {file_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

channel_raw = {
    "items": channel_data.get("items", []),
    "total_items": len(channel_data.get("items", [])),
    "extracted_at_utc": datetime.utcnow().isoformat()
}

playlist_raw = {
    "items": playlist_items,
    "total_items": len(playlist_items),
    "extracted_at_utc": datetime.utcnow().isoformat()
}

video_stats_raw = {
    "items": video_stats,
    "total_items": len(video_stats),
    "extracted_at_utc": datetime.utcnow().isoformat()
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_json_to_onelake(
    channel_raw,
    f"{RAW_BASE_PATH}/channel",
    "channel"
)

write_json_to_onelake(
    playlist_raw,
    f"{RAW_BASE_PATH}/playlist_items",
    "playlist_items"
)

write_json_to_onelake(
    video_stats_raw,
    f"{RAW_BASE_PATH}/video_stats",
    "video_stats"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
