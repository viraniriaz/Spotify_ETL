import functions_framework
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
from google.cloud import storage
import json
from datetime import datetime

@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """

    client_id = os.environ.get('CLIENT_ID')
    client_secret=os.environ.get('CLIENT_SECRET')
    client_credential_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp=spotipy.Spotify(client_credentials_manager=client_credential_manager)
    playlist_link="https://open.spotify.com/playlist/0zc6Hq9OIAengtGG6a3lfs"
    playlist_uri=playlist_link.split('/')[-1]
    data =sp.playlist_tracks(playlist_uri)
    storage_client = storage.Client()
    GCS_BUCKET_NAME = 'data_from_spotify'
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    filename='spotify_row_'+str(datetime.now())+'.json'
    blob = bucket.blob('raw_data/to_processed/'+filename)
    
    try:
        spotify_data_json = json.dumps(data)
    except Exception as e:
        return f"Error converting data to JSON: {str(e)}", 500
    try:
        blob.upload_from_string(spotify_data_json, content_type='application/json')
    except Exception as e:
        return f"Error uploading data to Google Cloud Storage: {str(e)}", 500
    return 'Success'

