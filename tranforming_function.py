import functions_framework
import logging
from google.cloud import storage
import json
import pandas as pd
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
    bucket_name = 'data_from_spotify'  # Replace with your GCS bucket name
    directory_path = 'raw_data/to_processed/'  # Replace with the directory path you want to list
    a=[]
    try:
        files = list_files_in_directory(bucket_name, directory_path)
        for file in files:
            a.append(file)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    
    spotify_data=[]
    for file_name in a[1:]:
        data = read_data_from_file(bucket_name, file_name)
        if data is not None:
            spotify_data.append(data)
    
    for data in spotify_data:
        album_list=album(data)
        artist_list=artist(data)
        song_list=songs(data)

    album_df=pd.DataFrame.from_dict(album_list)
    album_df=album_df.drop_duplicates(subset=['album_id'])

    artist_df=pd.DataFrame.from_dict(artist_list)
    artist_df=artist_df.drop_duplicates(subset=['artist_id'])

    song_df=pd.DataFrame.from_dict(song_list)
    song_df=song_df.drop_duplicates(subset=['song_id'])
        
    album_df['album_release_date']=pd.to_datetime(album_df['album_release_date'])
    

    
    storage_client = storage.Client()
    GCS_BUCKET_NAME = 'data_from_spotify'
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    filename='song_df_'+str(datetime.now())+'.csv'
    song_csv_data = song_df.to_csv(index=False)
    blob = bucket.blob('transformed/song_data/'+filename)
    blob.upload_from_string(song_csv_data, content_type='text/csv')

    filename='artist_df'+str(datetime.now())+'.csv'
    artist_csv_data = artist_df.to_csv(index=False)
    blob = bucket.blob('transformed/artist_data/'+filename)
    blob.upload_from_string(artist_csv_data, content_type='text/csv')

    filename='album_df'+str(datetime.now())+'.csv'
    album_csv_data = album_df.to_csv(index=False)
    blob = bucket.blob('transformed/album_data/'+filename)
    blob.upload_from_string(album_csv_data, content_type='text/csv')
    
    for file_name in a[1:]:
        result = copy_and_delete_data(bucket_name, 'raw_data/to_processed', 'raw_data/processed', file_name.split('/')[-1])
        if result.startswith('Error'):
            return result, 500

    return song_list


def list_files_in_directory(bucket_name, directory_path):
    # Initialize the Google Cloud Storage client
    storage_client = storage.Client()

    try:
        # Get the specified bucket
        bucket = storage_client.bucket(bucket_name)

        # List files in the specified directory
        files = []
        for blob in bucket.list_blobs(prefix=directory_path):
            # Add the file name to the list
            files.append(blob.name)

        return files
    except Exception as e:
        logging.error(f"An error occurred while listing files: {str(e)}")

def read_data_from_file(bucket_name, file_name):
    # Initialize the Google Cloud Storage client
    storage_client = storage.Client()

    try:
        # Get the specified bucket
        bucket = storage_client.bucket(bucket_name)

        # Get the blob (object) from the bucket
        blob = bucket.blob(file_name)

        # Download the data from the blob as text
        data = blob.download_as_text()
        json_data = json.loads(data)

        # Print the data
        logging.info(f"Data from file '{file_name}':")
        logging.info(data)

        return json_data
    except Exception as e:
        logging.error(f"An error occurred while reading file '{file_name}': {str(e)}")
        return None


def album(data):
    album_list=[]
    for row in data['items']:
        album_id=row['track']['album']['id']
        album_name=row['track']['album']['name']
        album_release_date=row['track']['album']['release_date']
        album_total_tracks=row['track']['album']['total_tracks']
        album_url=row['track']['album']['external_urls']['spotify']
        album_elements={'album_id':album_id,'album_name':album_name,'album_release_date':album_release_date,'album_total_tracks':album_total_tracks,'album_url':album_url}
        album_list.append(album_elements)
    return album_list


def artist(data):
    artist_list=[]
    for row in data['items']:
        for key,value in row.items():
            if key=='track':
                for artist in value['artists']:
                    artist_dict={'artist_id':artist['id'], 'artist_name':artist['name'], 'external_url':artist['href']}
                    artist_list.append(artist_dict)
    return artist_list


def songs(data):
    song_list=[]
    for row in data['items']:
        song_id=row['track']['id']
        song_name=row['track']['name']
        song_duration=row['track']['duration_ms']
        song_url=row['track']['external_urls']['spotify']
        song_popularity=row['track']['popularity']
        album_id=row['track']['album']['id']
        artist_id=row['track']['album']["artists"][0]['id']
        song_elements={'song_id':song_id,'song_name':song_name,'song_duration':song_duration,'song_url':song_url,'song_popularity':song_popularity,'album_id':song_popularity,'artist_id':song_popularity}
        song_list.append(song_elements)
    return song_list

def copy_and_delete_data(bucket_name, from_directory, to_directory, file_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # Copy the file from the 'from_directory' to the 'to_directory'
        source_blob = bucket.blob(from_directory + '/' + file_name)
        destination_blob = bucket.blob(to_directory + '/' + file_name)
        destination_blob.rewrite(source_blob)

        # Delete the file from the 'from_directory'
        source_blob.delete()

        return f'Successfully copied and deleted: {file_name}'
    except Exception as e:
        return f'Error copying and deleting: {file_name} - {str(e)}'
