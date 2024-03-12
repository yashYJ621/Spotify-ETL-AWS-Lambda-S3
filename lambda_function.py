import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime


def lambda_handler(event, context):
    
    client_id = "8430dac4c0cc41d580798dc2b97b4cd5"
    client_secret = "22dd2a849bbc4e40b63c7bf4f7d353f5"
    
    client_credentials_manager = SpotifyClientCredentials(client_id = client_id, client_secret = client_secret)
    sp = spotipy.spotify(client_credentials_manager = client_credentials_manager)
    playlists = sp.user_playlists('spotify')
    
    playlist_link = "https://open.spotify.com/playlist/37i9dQZF1DX5q67ZpWyRrZ?si=6dee0ff63faa4a12"
    playlist_uri = playlist_link.split("/")[-1].split("?")[0]
    
    spotify_data = sp.playlist_tracks(playlist_uri)
    
    client = boto3.client('s3')
    
    filename = "spotify_raw_" +str(datetime.now())+ ".json"
    
    client.put_object(
        Bucket = "spotify-yash-cloud-etl",
        Key = "data_raw/to_be_processed/" + filename,
        Body = json.dumps(spotify_data)
        )
